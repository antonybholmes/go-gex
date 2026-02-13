import argparse
import collections
import json
import os
import re
import sqlite3
import struct
import sys
from random import sample

import numpy as np
import pandas as pd

# from nanoid import generate
import uuid_utils as uuid
from importlib_metadata import metadata

DATA_TYPES = ["Counts", "TPM", "VST"]
DIR = "../data/modules/gex"
VERSION = 1


def load_sample_data(df: pd.DataFrame):  # , num_id_cols: int = 1):

    # id_names = df.columns.values[0:num_id_cols]
    sample_metadata_names = df.columns.values  # [num_id_cols:]

    sample_metadata_map = collections.defaultdict(lambda: collections.defaultdict(dict))

    sample_names = df.iloc[:, 0].values
    sample_id_map = {
        n: {"uuid": uuid.uuid7(), "index": i + 1} for i, n in enumerate(sample_names)
    }

    for i, row in df.iterrows():
        values = row.astype(str).values

        sample_name = sample_names[i]
        id = sample_id_map[sample_name]["uuid"]

        # ids = values[0:num_id_cols]

        # alt_id_names = id_names  # [1:]
        # alt_ids = ids  # [1:]

        # for name, alt_id in zip(id_names, ids):
        #     # here name is the column name, alt_id is the value
        #     # e.g. if a sample has multiple ids, e.g. GEO and SRA
        #     # then name is "GEO" and alt_id is "GSMxxxx" or
        #     # name is "SRA" and alt_id is "SRRxxxx"

        #     color = ""

        #     if "|" in alt_id:
        #         alt_id, color = alt_id.split("|")
        #         alt_id = alt_id.strip()
        #         color = color.strip()

        #     sample_id_map[sample_id][name] = alt_id  # {"id": alt_id, "color": color}

        values = values.astype(str)
        # values = values[num_id_cols:]

        for metadata_name, value in zip(sample_metadata_names, values):
            if value != "":
                color = ""

                # we can color ABC/GCB types differently if we want
                if "|" in value:
                    value, color = value.split("|")
                    value = value.strip()
                    color = color.strip()

                # if value not in sample_metadata_map[metadata_name]:
                #     sample_metadata_map[metadata_name][value] = {
                #         "color": color,
                #         "samples": [],
                #     }

                sample_metadata_map[sample_name][metadata_name] = {
                    "value": value,
                    "color": color,
                }

    # print(sample_names)

    return [
        sample_names,
        sample_id_map,
        sample_metadata_map,
    ]


def load_data(
    genome,
    sample_ids,
    data_type,
    file,
    id_col_count=1,
):
    # print(file, dataset_id)

    df = pd.read_csv(file, sep="\t", header=0, index_col=0, keep_default_na=False)

    if data_type == "RMA":
        probes = df.index.str.replace(r"\..+", "", regex=True).values
        genes = df.iloc[:, 0].str.replace(r"\..+", "", regex=True).values
        df = df.iloc[:, 1:]
    else:
        probes = df.index.str.replace(r"\..+", "", regex=True).values
        genes = df.index.str.replace(r"\..+", "", regex=True).values

    # if filter != "":
    #    df = df.iloc[:, np.where(df.columns.str.contains(filter, regex=True))[0]]

    # df = df.iloc[:, id_col_count:]

    # clean up column names
    df.columns = [re.sub(r"[ \|].+", "", str(c)) for c in df.columns]

    # only keep samples we have metadata for and reorder
    # print(df.columns)
    df = df[sample_ids]

    # print(df.shape)
    # exit(0)

    exp_map = collections.defaultdict(dict)

    probes_in_use = []

    for i, probe in enumerate(probes):

        gene = genes[i]

        # strip off version numbers from gene symbols

        # only keep genes we can match to hugo
        if gene not in gene_id_map[genome]:
            continue

        gene_id = gene_id_map[genome].get(gene, "")

        if gene_id == "":
            gene_id = prev_gene_id_map[genome].get(gene, "")

        if gene_id == "":
            gene_id = alias_gene_id_map[genome].get(gene, "")

        exp_map[probe] = {"gene": gene_id, "values": df.iloc[i, :].values}
        probes_in_use.append(probe)

    return probes_in_use, genes, exp_map


genes = []
gene_map = {}

datasets = []
dataset_map = {}


platforms = []
platformMap = {}

platforms.append({"name": "RNA-seq"})
platformMap["RNA-seq"] = len(platforms)

platforms.append({"name": "Microarray"})
platformMap["Microarray"] = len(platforms)


with open("datasets.json") as f:
    datasets = json.load(f)

print(datasets)


#
# Read gene symbols for matching
#

official_symbols = {"human": {}, "mouse": {}}

gene_ids = {"human": [], "mouse": []}
gene_id_map = {"human": {}, "mouse": {}}
prev_gene_id_map = {"human": {}, "mouse": {}}
alias_gene_id_map = {"human": {}, "mouse": {}}

metadata_map = {}

# gene_db_map = {}

file = (
    "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/hugo/hugo_20240524.tsv"
)
df_hugo = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

gene_index = 1

for i, gene_symbol in enumerate(df_hugo["Approved symbol"].values):

    # genes = [gene_id] + list(
    #     filter(
    #         lambda x: x != "",
    #         [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")],
    #     )
    # )

    hugo = df_hugo["HGNC ID"].values[i]
    ensembl = df_hugo["Ensembl gene ID"].values[i].split(".")[0]
    refseq = df_hugo["RefSeq IDs"].values[i].replace(" ", "")
    ncbi = df_hugo["NCBI Gene ID"].values[i].replace(" ", "")

    info = {
        "index": gene_index,
        "gene_id": hugo,
        "gene_symbol": gene_symbol,
        "ensembl": ensembl,
        "refseq": refseq,
        "ncbi": ncbi,
    }

    official_symbols["human"][hugo] = info

    gene_id_map["human"][hugo] = hugo
    gene_id_map["human"][gene_symbol] = hugo
    gene_id_map["human"][ensembl] = hugo
    gene_id_map["human"][refseq] = hugo
    gene_id_map["human"][ncbi] = hugo
    for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
        prev_gene_id_map["human"][g] = hugo

    for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
        alias_gene_id_map["human"][g] = hugo

    # gene_db_map[hugo] = hugo  # index
    # gene_db_map[gene_symbol] = index
    # gene_db_map[refseq] = index
    # gene_db_map[ncbi] = index

    # for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    # for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
    #     gene_db_map[g] = index

    gene_ids["human"].append(hugo)
    gene_index += 1

file = "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/mgi_entrez_ensembl_gene_list_20240531.tsv"
df_mgi = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

for i, gene_symbol in enumerate(df_mgi["gene_symbol"].values):

    mgi = df_mgi["mgi"].values[i]
    ensembl = df_mgi["ensembl"].values[i].split(".")[0].replace("null", "")
    refseq = df_mgi["refseq"].values[i].replace(" ", "").replace("null", "")
    ncbi = df_mgi["entrez"].values[i].replace(" ", "").replace("null", "")

    official_symbols["mouse"][mgi] = {
        "index": gene_index,
        "gene_id": mgi,
        "gene_symbol": gene_symbol,
        "ensembl": ensembl,
        "refseq": refseq,
        "ncbi": ncbi,
    }

    gene_id_map["mouse"][mgi] = mgi
    gene_id_map["mouse"][gene_symbol] = mgi
    gene_id_map["mouse"][refseq] = mgi
    gene_id_map["mouse"][ncbi] = mgi

    gene_index += 1
    # gene_db_map[mgi] = index
    # gene_db_map[gene_symbol] = index
    # gene_db_map[refseq] = index
    # gene_db_map[ncbi] = index

    gene_ids["mouse"].append(mgi)

#
# Write sql
#
db = f"../data/modules/gex/gex.db"

print(f"Writing to {db}")

rdfViewId = str(uuid.uuid7())


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")


cursor.execute(
    f"""
    CREATE TABLE genomes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        scientific_name TEXT NOT NULL,
        UNIQUE(name, scientific_name));
    """,
)
cursor.execute("CREATE INDEX idx_genomes_name ON genomes (LOWER(name));")

cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (1, '{uuid.uuid7()}', 'Human', 'Homo sapiens');"
)
cursor.execute(
    f"INSERT INTO genomes (id, public_id, name, scientific_name) VALUES (2, '{uuid.uuid7()}', 'Mouse', 'Mus musculus');"
)


cursor.execute(
    f"""
    CREATE TABLE genes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        gene_id TEXT NOT NULL,
        ensembl TEXT NOT NULL DEFAULT '',
        refseq TEXT NOT NULL DEFAULT '',
        ncbi INTEGER NOT NULL DEFAULT 0,
        gene_symbol TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(genome_id) REFERENCES genomes(id));
    """,
)
cursor.execute("CREATE INDEX idx_genes_gene_id ON genes (LOWER(gene_id));")
cursor.execute("CREATE INDEX idx_genes_ensembl ON genes (LOWER(ensembl));")
cursor.execute("CREATE INDEX idx_genes_refseq ON genes (LOWER(refseq));")
cursor.execute("CREATE INDEX idx_genes_gene_symbol ON genes (LOWER(gene_symbol));")
cursor.execute("CREATE INDEX idx_genes_genome_id ON genes(genome_id);")


cursor.execute(
    f"""
    CREATE TABLE technologies (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '');
    """,
)
cursor.execute("CREATE INDEX idx_technologies_name ON technologies (LOWER(name));")

cursor.execute(
    f"INSERT INTO technologies (id, public_id, name, description) VALUES (1, '{uuid.uuid7()}', 'RNA-seq', 'RNA sequencing');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name, description) VALUES (2, '{uuid.uuid7()}', 'Microarray', 'Microarray sequencing');"
)
cursor.execute(
    f"INSERT INTO technologies (id, public_id, name, description) VALUES (3, '{uuid.uuid7()}', 'scRNA-seq', 'Single-cell RNA sequencing');"
)

cursor.execute(
    f"""
    CREATE TABLE probes (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        technology_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        gene_id INTEGER NOT NULL,
        UNIQUE(genome_id, name),
        FOREIGN KEY(genome_id) REFERENCES genomes(id),
        FOREIGN KEY(technology_id) REFERENCES technologies(id),
        FOREIGN KEY(gene_id) REFERENCES genes(id));
    """,
)

cursor.execute("CREATE INDEX idx_probes_genome_id ON probes(genome_id);")
cursor.execute("CREATE INDEX idx_probes_technology_id ON probes(technology_id);")
cursor.execute("CREATE INDEX idx_probes_gene_id ON probes(gene_id);")

cursor.execute(
    f"""
    CREATE TABLE datasets (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        genome_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        technology_id INTEGER NOT NULL,
        platform TEXT NOT NULL,
        institution TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(genome_id) REFERENCES genomes(id),
        FOREIGN KEY(technology_id) REFERENCES technologies(id));
    """,
)

cursor.execute("CREATE INDEX idx_datasets_genome_id ON datasets(genome_id);")
cursor.execute("CREATE INDEX idx_datasets_technology_id ON datasets(technology_id);")

cursor.execute(
    f""" CREATE TABLE permissions (
	id INTEGER PRIMARY KEY ASC,
    public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f"INSERT INTO permissions (id, public_id, name) VALUES (1, '{rdfViewId}', 'rdf:view');"
)


cursor.execute(
    f""" CREATE TABLE dataset_permissions (
	dataset_id INTEGER,
    permission_id INTEGER,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id));
"""
)

cursor.execute(
    "CREATE INDEX idx_dataset_permissions_dataset_id ON dataset_permissions(dataset_id);"
)
cursor.execute(
    "CREATE INDEX idx_dataset_permissions_permission_id ON dataset_permissions(permission_id);"
)

cursor.execute(
    f"""
    CREATE TABLE samples (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        dataset_id INTEGER NOT NULL,
        name TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(dataset_id) REFERENCES datasets(id));
    """,
)

cursor.execute("CREATE INDEX idx_samples_dataset_id ON samples(dataset_id);")


cursor.execute(
    f"""
    CREATE TABLE metadata (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE,
        color TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '');
    """,
)

cursor.execute(
    f"""
    CREATE TABLE sample_metadata (
        sample_id INTEGER NOT NULL,
        metadata_id INTEGER NOT NULL,
        value TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(sample_id, metadata_id),
        FOREIGN KEY(sample_id) REFERENCES samples(id),
        FOREIGN KEY(metadata_id) REFERENCES metadata(id));
    """,
)

cursor.execute(
    "CREATE INDEX idx_sample_metadata_sample_id ON sample_metadata(sample_id);"
)
cursor.execute(
    "CREATE INDEX idx_sample_metadata_metadata_id ON sample_metadata(metadata_id);"
)

cursor.execute(
    f"""
    CREATE TABLE expression_types (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute(
    "CREATE INDEX idx_expression_types_name ON expression_types (LOWER(name));"
)

cursor.execute(
    f"""
    CREATE TABLE files (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        url TEXT NOT NULL UNIQUE);
    """,
)


cursor.execute(
    f"""
    CREATE TABLE data_types (
        id INTEGER PRIMARY KEY,
        public_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL UNIQUE);
    """,
)

cursor.execute("CREATE INDEX idx_data_types_name ON data_types (LOWER(name));")

cursor.execute(
    f"INSERT INTO data_types (id, public_id, name) VALUES (1, '{uuid.uuid7()}', 'float32');"
)

# the blob will store float32 values for all samples for a given gene/probe
cursor.execute(
    f"""
    CREATE TABLE expression (
        id INTEGER PRIMARY KEY,
        dataset_id INTEGER NOT NULL,
        probe_id TEXT NOT NULL,
        expression_type_id INTEGER NOT NULL,
        data_type_id INTEGER NOT NULL DEFAULT 1,
        offset INTEGER NOT NULL,
        length INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(dataset_id) REFERENCES datasets(id),
        FOREIGN KEY(expression_type_id) REFERENCES expression_types(id),
        FOREIGN KEY(probe_id) REFERENCES probes(id),
        FOREIGN KEY(data_type_id) REFERENCES data_types(id),
        FOREIGN KEY(file_id) REFERENCES files(id));
    """,
)

cursor.execute("CREATE INDEX idx_expression_dataset_id ON expression(dataset_id);")
cursor.execute(
    "CREATE INDEX idx_expression_expression_type_id ON expression(expression_type_id);"
)
cursor.execute("CREATE INDEX idx_expression_probe_id ON expression(probe_id);")
cursor.execute("CREATE INDEX idx_expression_data_type_id ON expression(data_type_id);")
cursor.execute("CREATE INDEX idx_expression_file_id ON expression(file_id);")


genomes = ["human", "mouse"]

genome_map = {"Human": 1, "Mouse": 2}

for si, s in enumerate(genomes):
    genome_id = si + 1
    for id in sorted(official_symbols[s]):
        d = official_symbols[s][id]

        cursor.execute(
            f"INSERT INTO genes (id, public_id, genome_id, gene_id, ensembl, refseq, ncbi, gene_symbol) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (
                d["index"],
                str(uuid.uuid7()),
                genome_id,
                d["gene_id"],
                d["ensembl"],
                d["refseq"],
                d["ncbi"],
                d["gene_symbol"],
            ),
        )


sample_index = 1
probe_index = 1
probe_map = {
    "human": collections.defaultdict(lambda: collections.defaultdict(int)),
    "mouse": collections.defaultdict(lambda: collections.defaultdict(int)),
}
expr_types = {}
file_map = {}
technology_map = {"RNA-seq": 1, "Microarray": 2, "scRNA-seq": 3}

for di, dataset in enumerate(datasets):
    dataset_index = di + 1
    dataset_id = str(uuid.uuid7())
    genome = dataset["genome"].lower()
    genome_id = genome_map[dataset["genome"]]
    technology = dataset["technology"]

    if technology not in technology_map:
        technology_map[technology] = len(technology_map) + 1
        cursor.execute(
            f"INSERT INTO technologies (id, public_id, name) VALUES ({technology_map[dataset['technology']]}, '{str(uuid.uuid7())}', '{dataset['technology']}');"
        )

    technology_id = technology_map[technology]

    cursor.execute(
        f"INSERT INTO datasets (id, public_id, genome_id, name, technology_id, platform, institution) VALUES ({dataset_index}, '{dataset_id}', {genome_id}, '{dataset['name']}', {technology_id}, '{dataset['platform']}', '{dataset['institution']}');",
    )

    cursor.execute(
        f"INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES ({dataset_index}, 1);"
    )

    df_samples = pd.read_csv(
        dataset["phenotypes"],
        sep="\t",
        header=0,
        keep_default_na=False,
    )

    sample_names, sample_id_map, sample_metadata_map = load_sample_data(df_samples)

    for sample_name in sample_names:
        id = str(uuid.uuid7())

        cursor.execute(
            f"INSERT INTO samples (id, public_id, dataset_id, name) VALUES ('{sample_index}', '{id}', '{dataset_index}', '{sample_name}');",
        )

        #
        # add metadata to sample
        #

        for m in sample_metadata_map[sample_name]:
            if m not in metadata_map:
                id = len(metadata_map) + 1
                metadata_map[m] = id

                cursor.execute(
                    f"""INSERT INTO metadata (id, public_id, name, color) VALUES ({id}, '{str(uuid.uuid7())}', '{m}', '{sample_metadata_map[sample_name][m]["color"]}');
                    """
                )

            metadata_id = metadata_map[m]

            cursor.execute(
                f"""INSERT INTO sample_metadata (sample_id, metadata_id, value) VALUES ({sample_index}, '{metadata_id}', '{sample_metadata_map[sample_name][m]["value"]}' );
                    """
            )

        sample_index += 1

    #
    # load exp data
    #

    for file in dataset["data"]:
        print(file["path"])
        probes, genes, exp_map = load_data(
            genome, sample_names, file["type"], file["path"], dataset["idColCount"]
        )

        exp_name = f"{dataset['name'].replace(" ", "_").replace("/", "_").lower()}"

        dir = f"{dataset['genome'].lower()}/{dataset['technology'].lower()}/{exp_name}"
        full_dir = os.path.join(DIR, dir)

        file_type = (
            file["type"]
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace(".", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("+", "_")
        )

        path = f"{dir}/{file_type}.bin"

        full_bin_path = os.path.join(DIR, path)

        expr_type = file["type"]

        print(expr_type)

        if expr_type not in expr_types:
            expr_types[expr_type] = len(expr_types) + 1
            cursor.execute(
                f"""INSERT INTO expression_types (id, public_id, name) VALUES ({expr_types[expr_type]}, '{str(uuid.uuid7())}', '{expr_type}');"""
            )
        expr_type_id = expr_types[expr_type]

        if path not in file_map:
            file_map[path] = len(file_map) + 1
            cursor.execute(
                f"""INSERT INTO files (id, public_id, url) VALUES ({file_map[path]}, '{str(uuid.uuid7())}', '{path}');"""
            )
        file_id = file_map[path]

        # 42, version, num probes, num samples, block size
        offset = 4 + 4 + 4 + 4 + 4
        num_samples = len(sample_names)

        lb = 4 + num_samples * 4  # bytes row occupies
        with open(full_bin_path, "wb") as fout:
            fout.write(struct.pack("<I", 42))
            fout.write(struct.pack("<I", VERSION))
            fout.write(struct.pack("<I", len(probes)))
            fout.write(struct.pack("<I", num_samples))
            fout.write(struct.pack("<I", lb))  # block size

            for probe in probes:
                if probe not in probe_map[genome][technology]:

                    gene_id = official_symbols[genome][exp_map[probe]["gene"]]["index"]

                    cursor.execute(
                        f"""INSERT INTO probes (id, public_id, genome_id, technology_id, gene_id, name) VALUES ({probe_index}, '{str(uuid.uuid7())}', {genome_id}, {technology_id}, {gene_id}, '{probe}');
                    """
                    )
                    probe_map[genome][technology][probe] = probe_index
                    probe_index += 1

                probe_id = probe_map[genome][technology][probe]

                # print(
                #     f"""INSERT INTO expression (dataset_id, probe_id, expression_type_id, offset, length, url, version) VALUES ({dataset_index}, {probe_id}, {expr_type_id}, {offset}, {l}, '{path}', 1);
                #     """
                # )

                cursor.execute(
                    f"""
                    INSERT INTO expression (dataset_id, probe_id, expression_type_id, offset, length, file_id, version) VALUES ({dataset_index}, {probe_id}, {expr_type_id}, {offset}, {num_samples}, {file_id}, {VERSION});
                    """
                )

                exp = exp_map[probe]

                # a block consists of the probe id followed by the expression values
                fout.write(struct.pack("<I", probe_id))
                fout.write(struct.pack("<" + "f" * num_samples, *(exp["values"])))

                # use 4 bytes per sample for float32
                offset += lb


# print(exp_map)


cursor.execute("CREATE INDEX idx_samples_dataset_name ON samples (LOWER(name));")
# -- CREATE INDEX idx_expr_gene_id_sample_id ON expr (gene_id);

# -- CREATE INDEX expr_type_public_id ON expr_types(public_id);

# CREATE INDEX idx_expr_sample_type_id ON expr(sample_id, expr_type_id);
# cursor.execute("CREATE INDEX idx_expr_gene_type_id ON expr(gene_id, expr_type_id);")

# cursor.execute("CREATE INDEX idx_metadata_types_name ON metadata_types(name);")

# cursor.execute("CREATE INDEX idx_metadata_type_id ON metadata (metadata_type_id);")
# -- CREATE INDEX idx_metadata_public_id ON metadata (public_id);


# cursor.execute("CREATE INDEX idx_permissions_name ON permissions (LOWER(name));")


# Commit and close
conn.commit()
conn.close()
