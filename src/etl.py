import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Set

import requests
import psycopg

DATABASE_URL = os.environ["DATABASE_URL"]
GROUP = os.environ.get("GROUP", "default")
METROS_PATH = os.environ.get("METROS_PATH", "metros.json")

ZIP_LOOKUP_URL = "https://comparepower.com/wp-admin/admin-ajax.php"
PLANS_CURRENT_URL = "https://pricing.api.comparepower.com/api/plans/current"

session = requests.Session()
session.headers.update({
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (compatible; ComparePowerETL/1.0)",
})

def load_metros(path: str) -> Dict[str, List[str]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_tdsp_for_zip(zip_code: str) -> Dict:
    params = {"action": "search_zipcode", "zipCode": zip_code}
    r = session.get(ZIP_LOOKUP_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"No TDSP data returned for ZIP {zip_code}. Response={data}")
    return data[0]

def get_plans_for_duns(duns: str, group: str) -> List[Dict]:
    params = {"group": group, "tdsp_duns": duns}
    r = session.get(PLANS_CURRENT_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def upsert_all(conn, zip_to_tdsp: Dict[str, Dict], plans_by_duns: Dict[str, List[Dict]], group: str) -> None:
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        # TDSP + ZIP map
        for z, tdsp in zip_to_tdsp.items():
            duns = str(tdsp.get("DUNS"))
            cur.execute("""
                INSERT INTO tdsp (duns, utility_id, utility_name, state, last_seen_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (duns) DO UPDATE SET
                  utility_id = EXCLUDED.utility_id,
                  utility_name = EXCLUDED.utility_name,
                  state = EXCLUDED.state,
                  last_seen_at = EXCLUDED.last_seen_at
            """, (duns, tdsp.get("UtilityID"), tdsp.get("UtilityName"), tdsp.get("State"), now))

            cur.execute("""
                INSERT INTO zip_tdsp_map (zip, duns, last_seen_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (zip) DO UPDATE SET
                  duns = EXCLUDED.duns,
                  last_seen_at = EXCLUDED.last_seen_at
            """, (z, duns, now))

        # Plans
        for duns, plans in plans_by_duns.items():
            for obj in plans:
                listing_id = obj.get("_id")
                prod = obj.get("product") or {}
                brand = prod.get("brand") or {}
                tdsp = obj.get("tdsp") or {}

                brand_id = brand.get("_id")
                product_id = prod.get("_id")
                tdsp_duns = str(tdsp.get("duns_number") or tdsp.get("_id") or duns)

                if brand_id:
                    cur.execute("""
                        INSERT INTO brand (id, name, puct_number, legal_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                          name = EXCLUDED.name,
                          puct_number = EXCLUDED.puct_number,
                          legal_name = EXCLUDED.legal_name
                    """, (brand_id, brand.get("name"), brand.get("puct_number"), brand.get("legal_name")))

                cur.execute("""
                    INSERT INTO tdsp (duns, utility_id, utility_name, state, last_seen_at)
                    VALUES (%s, NULL, %s, 'TX', %s)
                    ON CONFLICT (duns) DO UPDATE SET
                      utility_name = COALESCE(EXCLUDED.utility_name, tdsp.utility_name),
                      last_seen_at = EXCLUDED.last_seen_at
                """, (tdsp_duns, tdsp.get("name"), now))

                if product_id:
                    cur.execute("""
                        INSERT INTO product (
                          id, brand_id, name, term, family, percent_green, headline,
                          early_termination_fee, description, is_pre_pay, is_time_of_use
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (id) DO UPDATE SET
                          brand_id = EXCLUDED.brand_id,
                          name = EXCLUDED.name,
                          term = EXCLUDED.term,
                          family = EXCLUDED.family,
                          percent_green = EXCLUDED.percent_green,
                          headline = EXCLUDED.headline,
                          early_termination_fee = EXCLUDED.early_termination_fee,
                          description = EXCLUDED.description,
                          is_pre_pay = EXCLUDED.is_pre_pay,
                          is_time_of_use = EXCLUDED.is_time_of_use
                    """, (
                        product_id, brand_id, prod.get("name"), prod.get("term"), prod.get("family"),
                        prod.get("percent_green"), prod.get("headline"),
                        prod.get("early_termination_fee"), prod.get("description"),
                        prod.get("is_pre_pay"), prod.get("is_time_of_use")
                    ))

                cur.execute("""
                    INSERT INTO plan_listing (id, product_id, tdsp_duns, grp, fetched_at)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET
                      product_id = EXCLUDED.product_id,
                      tdsp_duns = EXCLUDED.tdsp_duns,
                      grp = EXCLUDED.grp,
                      fetched_at = EXCLUDED.fetched_at
                """, (listing_id, product_id, tdsp_duns, group, now))

                for ep in (obj.get("expected_prices") or []):
                    cur.execute("""
                        INSERT INTO expected_price (plan_listing_id, usage, price, actual, valid)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (plan_listing_id, usage) DO UPDATE SET
                          price = EXCLUDED.price,
                          actual = EXCLUDED.actual,
                          valid = EXCLUDED.valid
                    """, (listing_id, ep.get("usage"), ep.get("price"), ep.get("actual"), ep.get("valid")))

                for dl in (obj.get("document_links") or []):
                    cur.execute("""
                        INSERT INTO document_link (plan_listing_id, doc_type, language, link, snapshot_url)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (plan_listing_id, doc_type, language) DO UPDATE SET
                          link = EXCLUDED.link,
                          snapshot_url = EXCLUDED.snapshot_url
                    """, (listing_id, dl.get("type"), dl.get("language"), dl.get("link"), dl.get("snapshot_url")))

    conn.commit()

def main():
    metros = load_metros(METROS_PATH)
    all_zips = sorted({z for zs in metros.values() for z in zs})

    print(f"[{datetime.now()}] ZIPs: {len(all_zips)} group={GROUP}")

    zip_to_tdsp: Dict[str, Dict] = {}
    duns_set: Set[str] = set()

    # ZIP -> TDSP
    for z in all_zips:
        try:
            tdsp = get_tdsp_for_zip(z)
            zip_to_tdsp[z] = tdsp
            duns_set.add(str(tdsp["DUNS"]))
        except Exception as e:
            print(f"ZIP lookup failed {z}: {e}")
        time.sleep(0.15)

    if not duns_set:
        raise RuntimeError("No TDSP DUNS found from provided ZIPs. Check metros.json.")


    print(f"[{datetime.now()}] Unique DUNS: {len(duns_set)}")

    # DUNS -> plans/current
    plans_by_duns: Dict[str, List[Dict]] = {}
    for duns in sorted(duns_set):
        plans = get_plans_for_duns(duns, GROUP)
        plans_by_duns[duns] = plans
        print(f"DUNS {duns}: {len(plans)} plans")
        time.sleep(0.25)

    with psycopg.connect(DATABASE_URL) as conn:
        upsert_all(conn, zip_to_tdsp, plans_by_duns, GROUP)

    print(f"[{datetime.now()}] Done.")

if __name__ == "__main__":
    main()
