#!/usr/bin/env python3
"""
Generate synthetic NYC taxi trip data for local testing.
Produces a CSV file mimicking the NYC Yellow Taxi dataset schema.

Usage:
    python3 generate_sample_data.py [--rows 10000] [--output /path/to/output.csv]
    python3 generate_sample_data.py --rows 50000 --output /tmp/nyc_taxi_sample.csv
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta

# NYC taxi zones (simplified - just IDs 1-265)
LOCATION_IDS = list(range(1, 266))
VENDOR_IDS = [1, 2]
PAYMENT_TYPES = [1, 2, 3, 4]  # credit, cash, no_charge, dispute
RATE_CODES = [1, 2, 3, 4, 5, 6]

# High-traffic hours distribution (weighted)
HOUR_WEIGHTS = {
    0: 2, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2,
    6: 4, 7: 8, 8: 10, 9: 9, 10: 7, 11: 8,
    12: 9, 13: 8, 14: 7, 15: 7, 16: 9, 17: 11,
    18: 10, 19: 9, 20: 8, 21: 7, 22: 6, 23: 4,
}

HOURS = list(HOUR_WEIGHTS.keys())
HOUR_PROBS = [HOUR_WEIGHTS[h] / sum(HOUR_WEIGHTS.values()) for h in HOURS]

# Payment mix (realistic)
PAYMENT_PROBS = [0.65, 0.30, 0.03, 0.02]  # credit/cash/no_charge/dispute


def generate_trip(base_date: datetime) -> dict:
    """Generate a single realistic taxi trip record."""
    # Pickup time (weighted by hour)
    hour = random.choices(HOURS, weights=HOUR_PROBS)[0]
    day_offset = random.randint(0, 27)
    pickup = base_date + timedelta(
        days=day_offset,
        hours=hour,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    # Trip duration (log-normal to get realistic distribution)
    duration_minutes = max(2, random.lognormvariate(3.0, 0.8))
    duration_minutes = min(duration_minutes, 180)  # cap at 3 hours
    dropoff = pickup + timedelta(minutes=duration_minutes)

    # Distance (correlated with duration)
    speed_mph = random.uniform(5, 30)  # NYC traffic speed
    trip_distance = max(0.1, (duration_minutes / 60) * speed_mph)
    trip_distance = round(min(trip_distance, 50), 2)

    # Fare (NYC taxi formula: base $3.50 + $0.70/5th mile + extras)
    rate_code = random.choices(RATE_CODES, weights=[80, 5, 3, 2, 5, 5])[0]
    if rate_code == 1:  # Standard
        fare = 3.50 + (trip_distance * 2.50) + random.uniform(-0.5, 2.0)
    elif rate_code == 2:  # JFK flat
        fare = random.uniform(52, 60)
    else:
        fare = 3.50 + (trip_distance * 2.00)

    fare = round(max(3.0, fare), 2)

    # Payment
    payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_PROBS)[0]

    # Tip (only auto-populated for credit card)
    if payment_type == 1:
        tip_rate = random.choices(
            [0.0, 0.15, 0.18, 0.20, 0.25, 0.30],
            weights=[0.10, 0.15, 0.25, 0.30, 0.15, 0.05]
        )[0]
        tip = round(fare * tip_rate, 2)
    else:
        tip = 0.0

    extra = random.choices([0.0, 0.5, 1.0], weights=[0.3, 0.5, 0.2])[0]
    mta_tax = 0.5
    improvement = 0.3
    tolls = random.choices([0.0, 5.76, 6.12, 10.0], weights=[0.85, 0.06, 0.05, 0.04])[0]
    congestion = random.choices([0.0, 2.5, 2.75], weights=[0.2, 0.7, 0.1])[0]

    total = round(fare + tip + extra + mta_tax + improvement + tolls + congestion, 2)

    return {
        "VendorID": random.choice(VENDOR_IDS),
        "tpep_pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
        "tpep_dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": random.choices([1, 2, 3, 4, 5, 6], weights=[0.65, 0.15, 0.08, 0.05, 0.04, 0.03])[0],
        "trip_distance": trip_distance,
        "RatecodeID": rate_code,
        "store_and_fwd_flag": random.choices(["N", "Y"], weights=[0.98, 0.02])[0],
        "PULocationID": random.choice(LOCATION_IDS),
        "DOLocationID": random.choice(LOCATION_IDS),
        "payment_type": payment_type,
        "fare_amount": fare,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "improvement_surcharge": improvement,
        "total_amount": total,
        "congestion_surcharge": congestion,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic NYC taxi data")
    parser.add_argument("--rows", type=int, default=10000, help="Number of trip records")
    parser.add_argument(
        "--output",
        default="/tmp/nyc_taxi_sample.csv",
        help="Output CSV file path",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Base year for trip dates",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=1,
        help="Base month for trip dates",
    )
    args = parser.parse_args()

    base_date = datetime(args.year, args.month, 1)
    output_path = args.output

    print(f"Generating {args.rows:,} NYC taxi trips for {args.year}-{args.month:02d}...")

    FIELDNAMES = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge",
    ]

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for i in range(args.rows):
            if i % 10000 == 0 and i > 0:
                print(f"  {i:,} / {args.rows:,} rows...")
            writer.writerow(generate_trip(base_date))

    file_size = os.path.getsize(output_path)
    print(f"Done! Output: {output_path} ({file_size / 1024 / 1024:.1f} MB)")
    print(f"  Rows: {args.rows:,}")
    print(f"  Columns: {len(FIELDNAMES)}")

    # Print a quick stats summary
    print("\nQuick stats (reading back):")
    total_revenue = 0.0
    total_distance = 0.0
    credit_card_count = 0

    with open(output_path) as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            total_revenue += float(row["total_amount"])
            total_distance += float(row["trip_distance"])
            if row["payment_type"] == "1":
                credit_card_count += 1
            count += 1

    print(f"  Total revenue:     ${total_revenue:,.2f}")
    print(f"  Avg fare:          ${total_revenue / count:.2f}")
    print(f"  Total miles:       {total_distance:,.1f}")
    print(f"  Avg distance:      {total_distance / count:.2f} miles")
    print(f"  Credit card pct:   {credit_card_count / count * 100:.1f}%")


if __name__ == "__main__":
    main()
