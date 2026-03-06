"""
Exploratory script — Rentals United API
Calls Pull_ListReservations_RQ and prints what fields are available.

Per RU support:
  - Date ranges refer to booking creation or modification date
  - Results are limited to the past 7 days
  - Rate limit: 1 request per minute per method+params

Usage:
    python3 explore_rentals_united.py <password>
    python3 explore_rentals_united.py <password> 2026-02-01 2026-03-04
"""

import sys
import time
import xml.etree.ElementTree as ET
import requests
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

if len(sys.argv) < 2:
    print("Usage: python3 explore_rentals_united.py <password> [date_from] [date_to]")
    sys.exit(1)

# RU limits results to the past 7 days (creation/modification date)
_today  = date.today()
_7d_ago = _today - timedelta(days=7)

USERNAME  = "sales@merveil.co"
PASSWORD  = sys.argv[1]
DATE_FROM = sys.argv[2] if len(sys.argv) > 2 else _7d_ago.strftime("%Y-%m-%d")
DATE_TO   = sys.argv[3] if len(sys.argv) > 3 else _today.strftime("%Y-%m-%d")

# .NET DateTime formats to try
DATE_FROM_ISO  = DATE_FROM + "T00:00:00"
DATE_TO_ISO    = DATE_TO   + "T00:00:00"
DATE_FROM_US   = _7d_ago.strftime("%m/%d/%Y")
DATE_TO_US     = _today.strftime("%m/%d/%Y")

print(f"Date range: {DATE_FROM} → {DATE_TO}")

# ---------------------------------------------------------------------------
# API helper
# ---------------------------------------------------------------------------

ENDPOINT = "https://rm.rentalsunited.com/api/Handler.ashx"

def call(xml_body: str):
    resp = requests.post(
        ENDPOINT,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "text/xml; charset=utf-8"},
        timeout=60,
    )
    print(f"HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(resp.text[:1000])
        sys.exit(1)
    return ET.fromstring(resp.content), resp.text


AUTH = f"""<Authentication>
    <UserName>{USERNAME}</UserName>
    <Password>{PASSWORD}</Password>
  </Authentication>"""

# --- Check account has properties ---
XML_PROPS = f"""<Pull_ListAllProperties_RQ>
  {AUTH}
</Pull_ListAllProperties_RQ>"""

XML_V1 = f"""<Pull_ListReservations_RQ>
  {AUTH}
  <ModifiedFrom>{DATE_FROM}</ModifiedFrom>
  <ModifiedTo>{DATE_TO}</ModifiedTo>
</Pull_ListReservations_RQ>"""

XML_V2 = f"""<Pull_ListReservations_RQ>
  {AUTH}
  <DateFrom>{DATE_FROM_ISO}</DateFrom>
  <DateTo>{DATE_TO_ISO}</DateTo>
</Pull_ListReservations_RQ>"""

XML_V3 = f"""<Pull_ListReservations_RQ>
  {AUTH}
  <DateFrom>{DATE_FROM_US}</DateFrom>
  <DateTo>{DATE_TO_US}</DateTo>
</Pull_ListReservations_RQ>"""

# Step 0 — list properties to verify account is set up
print(f"\n{'='*60}")
print(f"  Step 0 — Pull_ListAllProperties_RQ")
print(f"{'='*60}")
root_p, raw_p = call(XML_PROPS)
error_p = root_p.find(".//error")
if error_p is not None:
    print(f"Error {error_p.get('ID')}: {error_p.text}")
else:
    props = root_p.findall(".//Property")
    print(f"Found {len(props)} propert(ies)")
    for p in props[:5]:
        pid = p.find("ID")
        name = p.find("Name")
        print(f"  PropertyID={pid.text if pid is not None else '?'}  Name={name.text if name is not None else '?'}")
    if len(props) == 0:
        print("Raw:", raw_p[:1000])

for i, (label, xml) in enumerate([
    ("ModifiedFrom/ModifiedTo",          XML_V1),
    ("DateFrom/DateTo (ISO + time)",     XML_V2),
    ("DateFrom/DateTo (MM/dd/yyyy)",     XML_V3),
]):
    if i > 0:
        print(f"\nWaiting 65s for rate limit...")
        time.sleep(65)

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    root, raw = call(xml)

    # Check for API-level error
    error = root.find(".//error")
    if error is not None:
        eid = error.get("ID")
        if eid == "-6":
            print("Rate limited — wait 1 minute and try again.")
        else:
            print(f"API error {eid}: {error.text}")
        continue

    status = root.find(".//Status")
    if status is not None:
        sid = status.get("ID", "?")
        print(f"API Status: {sid} — {status.text}")
        if sid != "0":
            print("Raw:", raw[:2000])
            continue

    reservations = root.findall(".//Reservation")
    print(f"Found {len(reservations)} reservation(s)")

    if len(reservations) == 0:
        print("\nRaw response (first 3000 chars):")
        print(raw[:3000])
        continue

    # Show first 3 reservations
    for j, res in enumerate(reservations[:3]):
        def field(tag):
            el = res.find(tag)
            return el.text if el is not None else "(absent)"

        print(f"\n--- Reservation {j+1} ---")
        print(f"  ReservationID   : {field('ReservationID')}")
        print(f"  PropertyID      : {field('PropertyID')}")
        print(f"  Channel         : {field('Channel')}")
        print(f"  StatusID        : {field('StatusID')}")
        print(f"  ArrivalDate     : {field('ArrivalDate')}")
        print(f"  DepartureDate   : {field('DepartureDate')}")
        print(f"  GuestName       : {field('GuestName')}")
        print(f"  Currency        : {field('Currency')}")
        print(f"  TotalAmount     : {field('TotalAmount')}")
        print(f"  CommissionAmount: {field('CommissionAmount')}")
        print(f"  PaymentMethod   : {field('PaymentMethod')}")

        if j == 0:
            print("\n  [All fields in first reservation]")
            for child in res.iter():
                if child.text and child.text.strip():
                    print(f"    <{child.tag}> = {child.text.strip()[:80]}")

    # Summary
    channels = {}
    for res in reservations:
        ch = res.find("Channel")
        if ch is not None and ch.text:
            channels[ch.text] = channels.get(ch.text, 0) + 1
    print(f"\nBy channel: {dict(sorted(channels.items(), key=lambda x: -x[1]))}")
    break  # stop at first variant that returns results
