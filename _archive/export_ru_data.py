"""
Export Rentals United data to CSV + JSON.
  - ru_export/reservations.csv   all reservations (flat)
  - ru_export/reservations.json  same, newline-delimited JSON
  - ru_export/pricing_<id>.json  pricing for first 3 unique properties

Usage:
    python3 export_ru_data.py <password>
    python3 export_ru_data.py <password> 2026-01-01 2026-03-05
"""

import csv
import json
import sys
import time
import xml.etree.ElementTree as ET
import requests
from datetime import date, timedelta
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python3 export_ru_data.py <password> [date_from] [date_to]")
    sys.exit(1)

_today  = date.today()
_7d_ago = _today - timedelta(days=7)

USERNAME  = "sales@merveil.co"
PASSWORD  = sys.argv[1]
DATE_FROM = (sys.argv[2] if len(sys.argv) > 2 else _7d_ago.strftime("%Y-%m-%d")) + "T00:00:00"
DATE_TO   = (sys.argv[3] if len(sys.argv) > 3 else _today.strftime("%Y-%m-%d"))   + "T00:00:00"

ENDPOINT = "https://rm.rentalsunited.com/api/Handler.ashx"
OUT_DIR  = Path("ru_export")
OUT_DIR.mkdir(exist_ok=True)

AUTH = f"""<Authentication>
    <UserName>{USERNAME}</UserName>
    <Password>{PASSWORD}</Password>
  </Authentication>"""


def call(xml_body: str) -> ET.Element:
    resp = requests.post(
        ENDPOINT,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8"},
        timeout=60,
    )
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}: {resp.text[:300]}")
        sys.exit(1)
    root = ET.fromstring(resp.content)
    error = root.find(".//error")
    if error is not None:
        eid = error.get("ID")
        if eid == "-6":
            print("Rate limited — waiting 65s...")
            time.sleep(65)
            return call(xml_body)
        print(f"API error {eid}: {error.text}")
        sys.exit(1)
    return root


def text(el: ET.Element, tag: str) -> str:
    node = el.find(f".//{tag}")
    return node.text.strip() if node is not None and node.text else ""


# ---------------------------------------------------------------------------
# Pull reservations
# ---------------------------------------------------------------------------

print(f"Fetching reservations {DATE_FROM} → {DATE_TO} ...")

root = call(f"""<Pull_ListReservations_RQ>
  {AUTH}
  <DateFrom>{DATE_FROM}</DateFrom>
  <DateTo>{DATE_TO}</DateTo>
</Pull_ListReservations_RQ>""")

reservations = root.findall(".//Reservation")
print(f"  → {len(reservations)} reservations")

FIELDS = [
    "ru_reservation_id", "status_id", "last_mod", "created_date", "is_archived",
    "property_id", "xml_apartment_id",
    "date_from", "date_to", "number_of_guests", "number_of_adults",
    "number_of_children", "number_of_infants", "number_of_pets",
    "ru_price", "client_price", "already_paid",
    "channel_total", "channel_rent", "channel_promotion", "channel_commission",
    "total",
    "creator", "reference_id", "pms_reservation_id", "stay_id",
    "guest_name", "guest_surname", "guest_email", "guest_phone",
    "guest_country_id", "cancel_type_id", "cancellation_policy",
    "comments",
]

rows = []
property_ids_seen = []

for res in reservations:
    pid = text(res, "PropertyID")
    if pid and pid not in property_ids_seen:
        property_ids_seen.append(pid)

    row = {
        "ru_reservation_id":   text(res, "ReservationID"),
        "status_id":           text(res, "StatusID"),
        "last_mod":            text(res, "LastMod"),
        "created_date":        text(res, "CreatedDate"),
        "is_archived":         text(res, "IsArchived"),
        "property_id":         pid,
        "xml_apartment_id":    text(res, "XmlApartmentID"),
        "date_from":           text(res, "DateFrom"),
        "date_to":             text(res, "DateTo"),
        "number_of_guests":    text(res, "NumberOfGuests"),
        "number_of_adults":    text(res, "NumberOfAdults"),
        "number_of_children":  text(res, "NumberOfChildren"),
        "number_of_infants":   text(res, "NumberOfInfants"),
        "number_of_pets":      text(res, "NumberOfPets"),
        "ru_price":            text(res, "RUPrice"),
        "client_price":        text(res, "ClientPrice"),
        "already_paid":        text(res, "AlreadyPaid"),
        "channel_total":       text(res, "ChannelTotal"),
        "channel_rent":        text(res, "ChannelRent"),
        "channel_promotion":   text(res, "ChannelPromotion"),
        "channel_commission":  text(res, "ChannelCommission"),
        "total":               text(res, "Total"),
        "creator":             text(res, "Creator"),
        "reference_id":        text(res, "ReferenceID"),
        "pms_reservation_id":  text(res, "PMSReservationId"),
        "stay_id":             text(res, "StayID"),
        "guest_name":          text(res, "Name"),
        "guest_surname":       text(res, "SurName"),
        "guest_email":         text(res, "Email"),
        "guest_phone":         text(res, "Phone"),
        "guest_country_id":    text(res, "CountryID"),
        "cancel_type_id":      text(res, "CancelTypeID"),
        "cancellation_policy": text(res, "CancellationPolicy"),
        "comments":            text(res, "Comments")[:200].replace("\n", " "),
    }
    rows.append(row)

# CSV
csv_path = OUT_DIR / "reservations.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)
print(f"  → saved {csv_path}")

# JSON (newline-delimited, BigQuery-ready)
json_path = OUT_DIR / "reservations.json"
with open(json_path, "w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
print(f"  → saved {json_path}")

# Channel summary
channels: dict = {}
for r in rows:
    ch = r["creator"]
    channels[ch] = channels.get(ch, 0) + 1
print("\n  Channel breakdown:")
for ch, n in sorted(channels.items(), key=lambda x: -x[1]):
    print(f"    {n:4d}  {ch}")

# ---------------------------------------------------------------------------
# Pull pricing for first 3 unique properties
# ---------------------------------------------------------------------------

print(f"\nFetching pricing for {min(3, len(property_ids_seen))} properties ...")

# Pricing window: next 90 days
price_from = _today.strftime("%Y-%m-%d")
price_to   = (_today + timedelta(days=90)).strftime("%Y-%m-%d")

for prop_id in property_ids_seen[:3]:
    print(f"\n  PropertyID {prop_id} ...")
    time.sleep(65)  # rate limit

    xml_rq = f"""<Pull_ListPropertyPrices_RQ>
  {AUTH}
  <PropertyID>{prop_id}</PropertyID>
  <DateFrom>{price_from}</DateFrom>
  <DateTo>{price_to}</DateTo>
</Pull_ListPropertyPrices_RQ>"""

    try:
        price_root = call(xml_rq)
    except SystemExit:
        print(f"    skipped (API error)")
        continue

    # Dump all unique tags and values
    price_data: dict = {"property_id": prop_id, "date_from": price_from, "date_to": price_to, "raw_fields": {}}
    seen_tags: set = set()
    day_prices = []

    for el in price_root.iter():
        tag = el.tag
        val = el.text.strip() if el.text and el.text.strip() else None
        if val and tag not in seen_tags:
            seen_tags.add(tag)
            price_data["raw_fields"][tag] = val

    # Try to extract per-day prices
    for day_el in price_root.findall(".//Day"):
        d = {child.tag: child.text.strip() if child.text else "" for child in day_el}
        if not d:
            d["text"] = day_el.text.strip() if day_el.text else ""
        day_prices.append(d)

    if not day_prices:
        # Maybe different structure — collect all Price elements
        for price_el in price_root.findall(".//Price"):
            d = {child.tag: child.text.strip() if child.text else "" for child in price_el}
            d["price_text"] = price_el.text.strip() if price_el.text else ""
            day_prices.append(d)

    price_data["daily_prices"] = day_prices[:10]  # first 10 days for preview
    price_data["total_price_entries"] = len(day_prices)

    out_path = OUT_DIR / f"pricing_{prop_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(price_data, f, indent=2, ensure_ascii=False)
    print(f"    → {len(day_prices)} price entries, saved {out_path}")
    print(f"    → Fields found: {list(seen_tags)}")

print(f"\nAll outputs in: {OUT_DIR}/")
