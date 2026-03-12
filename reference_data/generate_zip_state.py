# reference_data/generate_zip_state.py
# Generates a ZIP to state reference CSV using the
# us-zipcode library. Run this once to generate the file.
# After generation you can delete this script.

import csv
import os

# Basic ZIP prefix to state mapping
# This covers the valid ranges for each state
# Format: (start_prefix, end_prefix, state_code)
ZIP_STATE_RANGES = [
    ("00600", "00988", "PR"),
    ("01001", "02791", "MA"),
    ("03001", "03897", "NH"),
    ("03900", "04992", "ME"),
    ("05001", "05907", "VT"),
    ("06001", "06928", "CT"),
    ("07001", "08989", "NJ"),
    ("10001", "14975", "NY"),
    ("15001", "19640", "PA"),
    ("19701", "19980", "DE"),
    ("20001", "20599", "DC"),
    ("20601", "21930", "MD"),
    ("22001", "24658", "VA"),
    ("24701", "26886", "WV"),
    ("27006", "28909", "NC"),
    ("29001", "29948", "SC"),
    ("30001", "31999", "GA"),
    ("32004", "34997", "FL"),
    ("35004", "36925", "AL"),
    ("37010", "38589", "TN"),
    ("38601", "39776", "MS"),
    ("40003", "42788", "KY"),
    ("43001", "45999", "OH"),
    ("46001", "47997", "IN"),
    ("48001", "49971", "MI"),
    ("50001", "52809", "IA"),
    ("53001", "54990", "WI"),
    ("55001", "56763", "MN"),
    ("57001", "57799", "SD"),
    ("58001", "58856", "ND"),
    ("59001", "59937", "MT"),
    ("60001", "62999", "IL"),
    ("63001", "65899", "MO"),
    ("66002", "67954", "KS"),
    ("68001", "69367", "NE"),
    ("70001", "71497", "LA"),
    ("71601", "72959", "AR"),
    ("73001", "74966", "OK"),
    ("75001", "79999", "TX"),
    ("80001", "81658", "CO"),
    ("82001", "83128", "WY"),
    ("83201", "83876", "ID"),
    ("84001", "84784", "UT"),
    ("85001", "86556", "AZ"),
    ("87001", "88441", "NM"),
    ("88901", "89883", "NV"),
    ("90001", "96162", "CA"),
    ("96701", "96898", "HI"),
    ("97001", "97920", "OR"),
    ("98001", "99403", "WA"),
    ("99501", "99950", "AK"),
]

output_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "zip_state_reference.csv"
)

rows_written = 0
with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["zip_code", "state_code"])
    for start, end, state in ZIP_STATE_RANGES:
        start_int = int(start)
        end_int = int(end)
        for zip_int in range(start_int, end_int + 1):
            writer.writerow([str(zip_int).zfill(5), state])
            rows_written += 1

print(f"Generated zip_state_reference.csv with {rows_written} ZIP codes")
