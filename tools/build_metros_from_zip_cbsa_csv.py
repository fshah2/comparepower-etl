import csv
import json
from collections import defaultdict
from pathlib import Path

INPUT_CSV = Path("data/ZIP_CBSA.csv")
OUTPUT_JSON = Path("metros.json")

# Metro -> CBSA code
METROS = {
    "dfw": "19100",
    "houston": "26420",
    "san_antonio": "41700",
    "austin": "12420",
    "el_paso": "21340",
}

# Use TOT_RATIO by default; you can switch to RES_RATIO if you want.
RATIO_COL = "TOT_RATIO"

# 1% threshold is a good default. Lower includes fringe overlaps.
MIN_RATIO = 0.01

def main():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Missing file: {INPUT_CSV.resolve()}")

    metro_zips = defaultdict(set)

    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header row.")

        # Ensure required columns exist
        required = {"ZIP", "CBSA", RATIO_COL}
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(f"Missing columns {missing}. Found: {reader.fieldnames}")

        for row in reader:
            zip5 = (row.get("ZIP") or "").strip()
            cbsa = (row.get("CBSA") or "").strip()
            ratio_raw = (row.get(RATIO_COL) or "").strip()

            if not zip5 or not cbsa or not ratio_raw:
                continue

            # Normalize ZIP
            zip5 = zip5.zfill(5)

            try:
                ratio = float(ratio_raw)
            except ValueError:
                continue

            if ratio < MIN_RATIO:
                continue

            for metro_name, metro_cbsa in METROS.items():
                if cbsa == metro_cbsa:
                    metro_zips[metro_name].add(zip5)

    out = {k: sorted(v) for k, v in metro_zips.items()}
    OUTPUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"Used {RATIO_COL} >= {MIN_RATIO}")
    for metro_name in METROS:
        print(f"{metro_name}: {len(out.get(metro_name, []))} ZIPs")
    print(f"Wrote {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
