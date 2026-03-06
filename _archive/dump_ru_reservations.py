"""
Dump raw XML + channel summary for Rentals United reservations.
Saves first reservation per channel to separate files for inspection.

Usage:
    python3 dump_ru_reservations.py <password>
    python3 dump_ru_reservations.py <password> 2026-01-01 2026-03-04
"""

import sys
import xml.etree.ElementTree as ET
import requests
from datetime import date, timedelta
from pathlib import Path
import json

if len(sys.argv) < 2:
    print("Usage: python3 dump_ru_reservations.py <password> [date_from] [date_to]")
    sys.exit(1)

_today  = date.today()
_7d_ago = _today - timedelta(days=7)

USERNAME  = "sales@merveil.co"
PASSWORD  = sys.argv[1]
DATE_FROM = (sys.argv[2] if len(sys.argv) > 2 else _7d_ago.strftime("%Y-%m-%d")) + "T00:00:00"
DATE_TO   = (sys.argv[3] if len(sys.argv) > 3 else _today.strftime("%Y-%m-%d"))   + "T00:00:00"

print(f"Date range: {DATE_FROM} → {DATE_TO}")

ENDPOINT = "https://rm.rentalsunited.com/api/Handler.ashx"

AUTH = f"""<Authentication>
    <UserName>{USERNAME}</UserName>
    <Password>{PASSWORD}</Password>
  </Authentication>"""

XML_RQ = f"""<Pull_ListReservations_RQ>
  {AUTH}
  <DateFrom>{DATE_FROM}</DateFrom>
  <DateTo>{DATE_TO}</DateTo>
</Pull_ListReservations_RQ>"""

resp = requests.post(
    ENDPOINT,
    data=XML_RQ.encode("utf-8"),
    headers={"Content-Type": "text/xml; charset=utf-8"},
    timeout=60,
)
print(f"HTTP {resp.status_code}")
if resp.status_code != 200:
    print(resp.text[:500])
    sys.exit(1)

root = ET.fromstring(resp.content)

error = root.find(".//error")
if error is not None:
    print(f"API error {error.get('ID')}: {error.text}")
    sys.exit(1)

reservations = root.findall(".//Reservation")
print(f"Total reservations: {len(reservations)}")

# ---------------------------------------------------------------------------
# Channel summary + first reservation per channel
# ---------------------------------------------------------------------------

channels: dict = {}   # creator → count
first_per_channel: dict = {}  # creator → ET.Element

for res in reservations:
    creator_el = res.find(".//Creator")
    creator = creator_el.text.strip() if creator_el is not None and creator_el.text else "unknown"
    channels[creator] = channels.get(creator, 0) + 1
    if creator not in first_per_channel:
        first_per_channel[creator] = res

print("\n=== Channels ===")
for ch, count in sorted(channels.items(), key=lambda x: -x[1]):
    print(f"  {count:4d}  {ch}")

# ---------------------------------------------------------------------------
# Dump first reservation per channel — raw XML + financial fields
# ---------------------------------------------------------------------------

out_dir = Path("ru_dump")
out_dir.mkdir(exist_ok=True)

print(f"\n=== Financial fields per channel (first reservation) ===")

FINANCIAL_TAGS = [
    "RUPrice", "ClientPrice", "AlreadyPaid",
    "ChannelTotal", "ChannelRent", "ChannelPromotion", "ChannelCommission",
    "Rent", "Price", "Total",
    "CityTax", "TaxAmount", "Tax", "CleaningFee", "Deposit",
    "Currency", "CurrencyCode",
    "StatusID", "DateFrom", "DateTo", "NumberOfGuests",
    "XmlApartmentID", "PropertyID", "ReservationID", "ReferenceID",
    "Name", "SurName",
]

for creator, res in sorted(first_per_channel.items()):
    # Short label for file name
    label = creator.split("@")[0] if "@" in creator else creator
    label = label.replace(".", "_")

    # Save raw XML
    xml_str = ET.tostring(res, encoding="unicode")
    xml_path = out_dir / f"{label}.xml"
    xml_path.write_text(xml_str, encoding="utf-8")

    print(f"\n--- {creator} ---")
    seen_tags = set()
    for el in res.iter():
        tag = el.tag
        val = el.text.strip() if el.text and el.text.strip() else None
        if val and tag not in seen_tags:
            seen_tags.add(tag)
            is_financial = tag in FINANCIAL_TAGS
            marker = " ★" if is_financial else ""
            print(f"  <{tag}> = {val[:80]}{marker}")

    # Quick financial summary
    def g(tag):
        el = res.find(f".//{tag}")
        return el.text.strip() if el is not None and el.text else "(absent)"

    print(f"\n  Financial summary:")
    print(f"    ChannelTotal      = {g('ChannelTotal')}")
    print(f"    ChannelRent       = {g('ChannelRent')}")
    print(f"    ChannelPromotion  = {g('ChannelPromotion')}")
    print(f"    ChannelCommission = {g('ChannelCommission')}")
    print(f"    CityTax           = {g('CityTax')}")
    print(f"    CleaningFee       = {g('CleaningFee')}")
    print(f"    RUPrice           = {g('RUPrice')}")
    print(f"    ClientPrice       = {g('ClientPrice')}")
    print(f"    Raw XML saved → {xml_path}")

print(f"\nAll XMLs saved in: {out_dir}/")
