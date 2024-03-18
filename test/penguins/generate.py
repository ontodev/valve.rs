#!/usr/bin/env python3
import csv
import math
import random
import sys

random.seed(0)


columns = [
    "study_name",
    "sample_number",
    "species",
    "region",
    "island",
    "stage",
    "individual_id",
    "clutch_completion",
    "date_egg",
    "culmen_length",
    "culmen_depth",
    "flipper_length",
    "body_mass",
    "sex",
    "delta_15_n",
    "delta_13_c",
    "comments",
]

levels = ["error", "warn", "info"]

species = ["Adelie Penguin (Pygoscelis adeliae)"]
regions = ["Anvers"]
islands = ["Biscoe", "Dream", "Torgersen"]
stages = ["Adult, 1 Egg Stage"]
clutch_completions = ["Yes", "No"]
clutch_completion_weights = [90, 10]
sexes = ["MALE", "FEMALE", ""]
sex_weights = [48, 48, 4]


def randdate():
    year = random.randint(2007, 2009)
    month = random.randint(1, 12)
    day = random.randint(1, 30)
    return f"{year}-{month:02}-{day:02}"


def main():
    if len(sys.argv) < 2:
        count = int(1e3)
    else:
        try:
            count = int(sys.argv[1])
        except ValueError as e:
            print(f"Could not parse argument as integer: '{e}'")
            sys.exit(1)

    error_rate = 0.1
    error_count = math.floor(count * error_rate)
    error_rows = []
    for i in range(1, error_count):
        error_rows.append(random.randint(1, count))
    error_rows.sort()
    error_set = set(error_rows)

    with open("src/data/penguin.tsv", "w") as f:
        w = csv.DictWriter(f, columns, delimiter="\t", lineterminator="\n")
        w.writeheader()
        for i in range(1, count + 1):
            row = {
                "study_name": "FAKE123",
                "sample_number": i,
                "species": random.choice(species),
                "region": random.choice(regions),
                "island": random.choice(islands),
                "stage": random.choice(stages),
                "individual_id": f"N{math.floor(i / 2) + 1}A{i % 2 + 1}",
                "clutch_completion": random.choices(
                    clutch_completions,
                    weights=clutch_completion_weights
                )[0],
                "date_egg": randdate(),
                "culmen_length": random.randint(300, 500) / 10,
                "culmen_depth": random.randint(150, 230) / 10,
                "flipper_length": random.randint(160, 230),
                "body_mass": random.randint(1000, 5000),
                "sex": random.choices(sexes, weights=sex_weights)[0],
                "delta_15_n":
                f"{random.randint(700000, 1000000) / 100000:05}",
                "delta_13_c":
                f"{random.randint(-2700000, -2300000) / 100000:05}",
                "comments": None,
            }
            if i in error_set:
                row["sample_number"] = f"{i} foo"
            w.writerow(row)

    fieldnames = ["table", "row", "column", "level", "rule", "value",
                  "message"]
    with open("src/schema/message.tsv", "a") as f:
        w = csv.DictWriter(f, fieldnames, delimiter="\t", lineterminator="\n")
        for i in error_rows:
            row = {
                "table": "penguin",
                "row": i,
                # "column": random.choice(columns),
                # "level": random.choice(levels),
                "column": "Sample Number",
                "level": "error",
                "rule": "test",
                "value": f"{i} foo",
                "message": "Test message",
            }
            w.writerow(row)


if __name__ == "__main__":
    main()
