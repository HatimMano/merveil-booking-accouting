"""
Join Mews (BQ export) with RU reservations CSV and produce a comparison CSV.

Inputs:
    ru_export/mews_reservations.csv   — BQ export from the query above
    ru_export/reservations.csv        — RU API export (already generated)

Output:
    ru_export/comparison_mews_ru.csv

Join key: mews.channel_number == ru.reference_id

Usage:
    python3 compare_mews_ru.py
"""

import csv
from pathlib import Path

MEWS_CSV = Path("ru_export/mews_reservations.csv")
RU_CSV   = Path("ru_export/reservations.csv")
OUT_CSV  = Path("ru_export/comparison_mews_ru.csv")

if not MEWS_CSV.exists():
    print(f"Missing: {MEWS_CSV}")
    print("Export Mews data from BQ using the query in the README and save to that path.")
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Load RU data indexed by reference_id
# ---------------------------------------------------------------------------
ru_by_ref: dict = {}
with open(RU_CSV, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        ref = row["reference_id"].strip()
        if ref:
            ru_by_ref[ref] = row

print(f"RU reservations loaded: {len(ru_by_ref)}")

# ---------------------------------------------------------------------------
# Load Mews, join with RU, write comparison
# ---------------------------------------------------------------------------

COLS = [
    # --- join key ---
    "booking_ref",
    "match",                        # found in both / mews only / ru only

    # --- identifiers ---
    "mews_reservation_id",
    "mews_number",
    "ru_reservation_id",

    # --- status ---
    "mews_status",
    "ru_status_id",

    # --- dates ---
    "checkin_date",
    "checkout_date",
    "nights",
    "mews_created_at",
    "ru_created_at",
    "mews_updated_at",
    "ru_last_mod",

    # --- channel ---
    "mews_booking_channel",
    "mews_booking_origin",
    "ru_creator",

    # --- property ---
    "mews_apartment_name",
    "mews_apartment_code",
    "ru_xml_apartment_id",
    "ru_property_id",

    # --- guest ---
    "mews_customer_name",
    "ru_guest_name",
    "mews_customer_nationality",
    "ru_guest_country_id",
    "mews_person_count",
    "ru_number_of_guests",

    # --- Mews financials ---
    "mews_accom_gross",
    "mews_accom_net",
    "mews_product_gross",           # cleaning fee?
    "mews_total_gross",
    "mews_total_net",
    "mews_adr",

    # --- RU financials (not in Mews) ---
    "ru_price",
    "ru_client_price",
    "ru_channel_total",
    "ru_channel_rent",
    "ru_channel_commission",
    "ru_channel_promotion",
    "ru_already_paid",

    # --- computed cross-checks ---
    "delta_total_ru_vs_mews",       # ru_price - mews_total_gross  (should be ~0)
    "commission_pct",               # channel_commission / channel_rent * 100
    "channel_total_minus_ru_price", # guest fee or city tax
    "ru_net_after_commission",      # channel_rent - channel_commission (= ru_price for Airbnb)
    "mews_implicit_tax_pct",        # (total_gross - total_net) / total_gross * 100
]


def safe_float(v: str) -> float | None:
    try:
        return float(v) if v and v.strip() else None
    except ValueError:
        return None


def pct(a, b):
    if a is None or b is None or b == 0:
        return ""
    return round(a / b * 100, 2)


rows_out = []
matched = unmatched_mews = 0

with open(MEWS_CSV, newline="", encoding="utf-8") as f:
    for m in csv.DictReader(f):
        ref = m.get("channel_number", "").strip()
        ru  = ru_by_ref.get(ref)

        if ru:
            matched += 1
            status = "MATCH"
        else:
            unmatched_mews += 1
            status = "MEWS_ONLY"
            ru = {}

        # financials
        mews_total   = safe_float(m.get("total_revenue_gross"))
        mews_net     = safe_float(m.get("total_revenue_net"))
        ru_price     = safe_float(ru.get("ru_price"))
        ch_rent      = safe_float(ru.get("channel_rent"))
        ch_comm      = safe_float(ru.get("channel_commission"))
        ch_total     = safe_float(ru.get("channel_total"))

        delta = round(ru_price - mews_total, 2) if (ru_price is not None and mews_total is not None) else ""
        net_after_comm = round(ch_rent - ch_comm, 2) if (ch_rent is not None and ch_comm is not None) else ""
        ch_minus_ru = round(ch_total - ru_price, 2) if (ch_total is not None and ru_price is not None) else ""
        mews_tax_pct = pct((mews_total - mews_net) if (mews_total and mews_net) else None, mews_total)

        rows_out.append({
            "booking_ref":               ref,
            "match":                     status,
            "mews_reservation_id":       m.get("reservation_id", ""),
            "mews_number":               m.get("reservation_number", ""),
            "ru_reservation_id":         ru.get("ru_reservation_id", ""),
            "mews_status":               m.get("status", ""),
            "ru_status_id":              ru.get("status_id", ""),
            "checkin_date":              m.get("checkin_date", ""),
            "checkout_date":             m.get("checkout_date", ""),
            "nights":                    m.get("nights", ""),
            "mews_created_at":           m.get("created_at", ""),
            "ru_created_at":             ru.get("created_date", ""),
            "mews_updated_at":           m.get("updated_at", ""),
            "ru_last_mod":               ru.get("last_mod", ""),
            "mews_booking_channel":      m.get("booking_channel", ""),
            "mews_booking_origin":       m.get("booking_origin", ""),
            "ru_creator":                ru.get("creator", ""),
            "mews_apartment_name":       m.get("apartment_name", ""),
            "mews_apartment_code":       m.get("apartment_code", ""),
            "ru_xml_apartment_id":       ru.get("xml_apartment_id", ""),
            "ru_property_id":            ru.get("property_id", ""),
            "mews_customer_name":        m.get("customer_name", ""),
            "ru_guest_name":             f"{ru.get('guest_name','')} {ru.get('guest_surname','')}".strip(),
            "mews_customer_nationality": m.get("customer_nationality", ""),
            "ru_guest_country_id":       ru.get("guest_country_id", ""),
            "mews_person_count":         m.get("person_count", ""),
            "ru_number_of_guests":       ru.get("number_of_guests", ""),
            "mews_accom_gross":          m.get("accommodation_revenue_gross", ""),
            "mews_accom_net":            m.get("accommodation_revenue_net", ""),
            "mews_product_gross":        m.get("product_revenue_gross", ""),
            "mews_total_gross":          m.get("total_revenue_gross", ""),
            "mews_total_net":            m.get("total_revenue_net", ""),
            "mews_adr":                  m.get("adr", ""),
            "ru_price":                  ru.get("ru_price", ""),
            "ru_client_price":           ru.get("client_price", ""),
            "ru_channel_total":          ru.get("channel_total", ""),
            "ru_channel_rent":           ru.get("channel_rent", ""),
            "ru_channel_commission":     ru.get("channel_commission", ""),
            "ru_channel_promotion":      ru.get("channel_promotion", ""),
            "ru_already_paid":           ru.get("already_paid", ""),
            "delta_total_ru_vs_mews":    delta,
            "commission_pct":            pct(ch_comm, ch_rent),
            "channel_total_minus_ru_price": ch_minus_ru,
            "ru_net_after_commission":   net_after_comm,
            "mews_implicit_tax_pct":     mews_tax_pct,
        })

# RU-only rows (in RU but not matched in Mews)
ru_only = 0
matched_refs = {r["booking_ref"] for r in rows_out if r["match"] == "MATCH"}
for ref, ru in ru_by_ref.items():
    if ref not in matched_refs:
        ru_only += 1
        rows_out.append({
            "booking_ref": ref,
            "match": "RU_ONLY",
            "ru_reservation_id": ru.get("ru_reservation_id", ""),
            "ru_status_id":      ru.get("status_id", ""),
            "checkin_date":      ru.get("date_from", ""),
            "checkout_date":     ru.get("date_to", ""),
            "ru_created_at":     ru.get("created_date", ""),
            "ru_last_mod":       ru.get("last_mod", ""),
            "ru_creator":        ru.get("creator", ""),
            "ru_xml_apartment_id": ru.get("xml_apartment_id", ""),
            "ru_property_id":    ru.get("property_id", ""),
            "ru_guest_name":     f"{ru.get('guest_name','')} {ru.get('guest_surname','')}".strip(),
            "ru_guest_country_id": ru.get("guest_country_id", ""),
            "ru_number_of_guests": ru.get("number_of_guests", ""),
            "ru_price":          ru.get("ru_price", ""),
            "ru_client_price":   ru.get("client_price", ""),
            "ru_channel_total":  ru.get("channel_total", ""),
            "ru_channel_rent":   ru.get("channel_rent", ""),
            "ru_channel_commission": ru.get("channel_commission", ""),
            "ru_channel_promotion":  ru.get("channel_promotion", ""),
            "ru_already_paid":   ru.get("already_paid", ""),
        })

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows_out)

print(f"\n=== Summary ===")
print(f"  Matched (both Mews + RU) : {matched}")
print(f"  Mews only (no RU match)  : {unmatched_mews}")
print(f"  RU only (no Mews match)  : {ru_only}")
print(f"\n  → {OUT_CSV}")
