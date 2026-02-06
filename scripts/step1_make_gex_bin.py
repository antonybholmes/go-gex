import argparse
import gzip
import json
import os
import struct
import sys
from os import path

import msgpack
import numpy as np
import pandas as pd
from nanoid import generate
from scipy import sparse

VERSION = 1

DIR = "../data/modules/gex"


def write_entries(block: int, genes: int, buffer: bytes):
    fout = path.join(dir, f"gex_{block}.bin")

    with open(fout, "wb") as f:
        f.write(struct.pack("<I", 42))  # magic
        f.write(struct.pack("<I", VERSION))  # version
        f.write(struct.pack("<I", genes))  # number of records
        # f.write(struct.pack("<I", len(offsets)))  # number of entries
        # for offset in offsets:
        #    f.write(struct.pack("<I", offset[0]))  # 4 bytes each offset
        #    f.write(struct.pack("<I", offset[1]))  # 4 bytes each size
        f.write(buffer)


with open("datasets.json") as f:
    datasets = json.load(f)

for dataset in datasets:
    id = dataset["name"].replace(" ", "_").replace("/", "_").lower()

    for file in dataset["data"]:
        dir = f"{dataset['genome'].lower()}/{dataset['technology'].lower()}/{id}"
        full_dir = os.path.join(DIR, dir)
        path = f"{dir}/{file['type'].lower()}.bin"

        full_path = os.path.join(DIR, path)

        print(path, file["path"])

        os.makedirs(full_dir, exist_ok=True)

        df = pd.read_csv(file["path"], sep="\t", header=0)
        cols = dataset["idColCount"]

        df = df.iloc[:, cols:]

        print(df.shape)

        buf = bytearray()

        buf += struct.pack("<I", 42)
        buf += struct.pack("<I", VERSION)
        buf += struct.pack("<I", df.shape[0])
        buf += struct.pack("<I", df.shape[1])
        cols = df.shape[1]
        for i, row in df.iterrows():
            # print(row.values)
            buf += struct.pack("<" + "f" * cols, *row.values)

        with open(full_path, "wb") as f:
            f.write(buf)
