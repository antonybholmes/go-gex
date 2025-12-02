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

conn = sqlite3.connect(os.path.join(dir, "gex.db"))
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute("DROP TABLE IF EXISTS datasets;")
cursor.execute(
    """ CREATE TABLE datasets (
	id TEXT PRIMARY KEY ASC,
	species TEXT NOT NULL,
	technology TEXT NOT NULL,
	platform TEXT NOT NULL,
	institution TEXT NOT NULL,
	name TEXT NOT NULL UNIQUE,
	path TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '');
    """
)

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")
for row in data:
    values = ", ".join([f"'{v}'" for v in row])
    cursor.execute(
        f"INSERT INTO datasets (id, species, technology, platform, institution, name, description, path) VALUES ({values});",
    )

cursor.execute("COMMIT;")

conn.commit()
conn.close()
