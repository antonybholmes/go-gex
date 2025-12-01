# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3
from nanoid import generate

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="sample name")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []

for root, dirs, files in os.walk(dir):
    for filename in files:
        if "gex.db" not in filename and filename.endswith(".db"):
            relative_dir = root.replace(dir, "")[1:]

            print(relative_dir)

            # species, platform, dataset = relative_dir.split("/")

            # filepath = os.path.join(root, filename)
            # print(root, filename, relative_dir, platform, species, dataset,)

            path = os.path.join(relative_dir, filename)

            conn = sqlite3.connect(os.path.join(root, filename))

            print(filename)

            # Create a cursor object
            cursor = conn.cursor()

            # Execute a query to fetch data
            cursor.execute(
                "SELECT id, species, technology, platform, institution, name, description FROM dataset"
            )

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = list(row)
                # row.append(generate("0123456789abcdefghijklmnopqrstuvwxyz", 12))
                # row.append(dataset)
                # row.append("db")
                row.append(path)
                # row.append(dataset)
                data.append(row)

            conn.close()

with open(os.path.join(dir, "gex.sql"), "w") as f:
    print("BEGIN TRANSACTION;", file=f)
    for row in data:
        values = ", ".join([f"'{v}'" for v in row])
        print(
            f"INSERT INTO datasets (id, species, technology, platform, institution, name, description, path) VALUES ({values});",
            file=f,
        )

    print("COMMIT;", file=f)
