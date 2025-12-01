import collections
import re
import struct
import sys
import pandas as pd
import numpy as np

# from nanoid import generate
import uuid_utils as uuid
import argparse


DATA_TYPES = ["Counts", "TPM", "VST"]


def get_file_id(name: str) -> str:
    return re.sub(r"[\/ ]+", "_", name.lower())


def load_sample_data(df: pd.DataFrame):  # , num_id_cols: int = 1):

    # id_names = df.columns.values[0:num_id_cols]
    sample_metadata_names = df.columns.values  # [num_id_cols:]

    sample_metadata_map = collections.defaultdict(lambda: collections.defaultdict(str))

    sample_names = df.iloc[:, 0].values
    sample_id_map = {n: uuid.uuid7() for n in sample_names}

    for i, row in df.iterrows():
        values = row.astype(str).values

        sample_name = sample_names[i]
        id = sample_id_map[sample_name]

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

                if "|" in value:
                    value, color = value.split("|")
                    value = value.strip()
                    color = color.strip()

                if value not in sample_metadata_map[metadata_name]:
                    sample_metadata_map[metadata_name][value] = {
                        "color": color,
                        "samples": [],
                    }

                sample_metadata_map[metadata_name][value]["samples"].append(id)

    print(sample_names)

    return [
        sample_names,
        sample_id_map,
        sample_metadata_names,
        sample_metadata_map,
    ]


def load_data(
    sample_ids,
    data_type,
    file,
    dataset_id,
    exp_map,
    filter="",
):
    print(file, dataset_id)

    df = pd.read_csv(file, sep="\t", header=0, index_col=0, keep_default_na=False)

    if data_type == "RMA":
        probes = df.index.values
        genes = df.iloc[:, 0].values
        df = df.iloc[:, 1:]
    else:
        probes = df.index.values
        genes = df.index.values

    if filter != "":
        df = df.iloc[:, np.where(df.columns.str.contains(filter, regex=True))[0]]

    # clean up column names
    df.columns = [re.sub(r"[ \|].+", "", str(c)) for c in df.columns]

    # only keep samples we have metadata for and reorder
    print(df.columns)
    df = df[sample_ids]

    print(df.shape)

    # print(df.shape)
    # exit(0)

    for i, probe in enumerate(probes):
        gene = genes[i]

        # strip off version numbers from gene symbols
        gene = gene.split(".")[0]

        # only keep genes we can match to hugo
        if gene not in gene_id_map:
            continue

        gene_id = gene_id_map.get(gene, "")

        if gene_id == "":
            gene_id = prev_gene_id_map.get(gene, "")

        if gene_id == "":
            gene_id = alias_gene_id_map.get(gene, "")

        exp_map[dataset_id][probe][gene_id][data_type] = df.iloc[i, :].values


exp_map = collections.defaultdict(
    lambda: collections.defaultdict(
        lambda: collections.defaultdict(lambda: collections.defaultdict(str))
    )
)

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


# Create an argument parser
parser = argparse.ArgumentParser(description="Make GEX sql file")

# Add named arguments
parser.add_argument("--name", type=str, help="Dataset name", required=True)
parser.add_argument(
    "--institution", type=str, help="Where data came from", required=True
)
parser.add_argument("--phenotypes", type=str, help="Phenotypes file", required=True)
parser.add_argument(
    "--filetype", type=str, help="filetype=file, e.g. tpm=tpm.txt", action="append"
)
# parser.add_argument("--tpm", type=str, help="TPM file")
# parser.add_argument("--vst", type=str, help="VST file")
# parser.add_argument("--rma", type=str, help="RMA file")
parser.add_argument("--id_col_count", type=int, help="How many id columns", default=1)
parser.add_argument(
    "--technology",
    type=str,
    help="Sequencing technology, e.g. RNA-seq",
    default="RNA-seq",
)
parser.add_argument(
    "--platform", type=str, help="Sequencing platform, e.g. HG-U133_Plus_2", default=""
)
parser.add_argument("--species", type=str, help="Species, e.g. Human", default="Human")

# Parse the command line arguments
args = parser.parse_args()

filetypes = [
    {"type": ft.split(",")[0], "file": ft.split(",")[1]} for ft in args.filetype
]


#
# Read gene symbols for matching
#

official_symbols = {}

gene_ids = []
gene_id_map = {}
prev_gene_id_map = {}
alias_gene_id_map = {}
# gene_db_map = {}

if args.species == "Mouse":
    file = "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/mgi_entrez_ensembl_gene_list_20240531.tsv"
    df_mgi = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

    for i, gene_symbol in enumerate(df_mgi["gene_symbol"].values):

        mgi = df_mgi["mgi"].values[i]
        ensembl = df_mgi["ensembl"].values[i].split(".")[0]
        refseq = df_mgi["refseq"].values[i].replace(" ", "")
        ncbi = df_mgi["entrez"].values[i].replace(" ", "")

        official_symbols[mgi] = {
            "id": mgi,
            "gene_symbol": gene_symbol,
            "ensembl": ensembl,
            "refseq": refseq,
            "ncbi": ncbi,
        }

        gene_id_map[mgi] = mgi
        gene_id_map[gene_symbol] = mgi
        gene_id_map[refseq] = mgi
        gene_id_map[ncbi] = mgi

        index = i + 1
        # gene_db_map[mgi] = index
        # gene_db_map[gene_symbol] = index
        # gene_db_map[refseq] = index
        # gene_db_map[ncbi] = index

        gene_ids.append(mgi)
else:
    file = "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/hugo/hugo_20240524.tsv"
    df_hugo = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

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

        official_symbols[hugo] = {
            "id": hugo,
            "gene_symbol": gene_symbol,
            "ensembl": ensembl,
            "refseq": refseq,
            "ncbi": ncbi,
        }

        gene_id_map[hugo] = hugo
        gene_id_map[gene_symbol] = hugo
        gene_id_map[ensembl] = hugo
        gene_id_map[refseq] = hugo
        gene_id_map[ncbi] = hugo

        for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
            prev_gene_id_map[g] = hugo

        for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
            alias_gene_id_map[g] = hugo

        # gene_db_map[hugo] = hugo  # index
        # gene_db_map[gene_symbol] = index
        # gene_db_map[refseq] = index
        # gene_db_map[ncbi] = index

        # for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
        #     gene_db_map[g] = index

        # for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
        #     gene_db_map[g] = index

        gene_ids.append(hugo)


#
# Read sample data
#

df_samples = pd.read_csv(
    args.phenotypes,
    sep="\t",
    header=0,
    keep_default_na=False,
)


sample_names, sample_id_map, sample_metadata_names, sample_metadata_map = (
    load_sample_data(df_samples)
)

# print(sample_id_map)
# print(sample_metadata_map)


# samples = []
# sample_map = {}

dataset_name = args.name
file_id = get_file_id(dataset_name)

# f"{args.technology.lower()}:{uuid.uuid7()}"  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
dataset = {
    "dataset_id": uuid.uuid7(),
    "name": dataset_name,
    "institution": args.institution,
    "technology": args.technology,
    "platform": args.platform,
    "species": args.species,
}

#
# Write sql
#

with open(
    f"../data/modules/gex/{args.species}/{args.technology}/{file_id}.sql", "w"
) as f:
    print("BEGIN TRANSACTION;", file=f)

    print(
        f"INSERT INTO dataset (id, name, species, institution, technology, platform) VALUES ('{dataset['dataset_id']}', '{dataset['name']}', '{dataset['species']}', '{dataset['institution']}', '{dataset['technology']}', '{dataset['platform']}');",
        file=f,
    )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for i, sample_name in enumerate(sample_names):
        id = sample_id_map[sample_name]
        print(
            f"INSERT INTO samples (id, name) VALUES ('{id}', '{sample_name}');",
            file=f,
        )

    print("COMMIT;", file=f)

    # print("BEGIN TRANSACTION;", file=f)

    # sample_db_ids = {sample: i + 1 for i, sample in enumerate(sample_ids)}

    # for i, sample in enumerate(sample_ids):
    #     for metadata_name in alt_id_names:
    #         value = sample_id_map[sample][metadata_name]

    #         print(
    #             f"INSERT INTO sample_alt_names (sample_id, name, value) VALUES ({i + 1}, '{metadata_name}', '{value}');",
    #             file=f,
    #         )

    # print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    metadata_type_map = {}
    for metadata_name in sample_metadata_names:
        id = uuid.uuid7()

        print(
            f"INSERT INTO metadata_types (id, name ) VALUES ('{id}', '{metadata_name}');",
            file=f,
        )

        metadata_type_map[metadata_name] = id

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    metadata_map = collections.defaultdict(lambda: collections.defaultdict(str))

    for mi, metadata_name in enumerate(sample_metadata_names):
        metadata_type_id = metadata_type_map[metadata_name]

        for value in sorted(sample_metadata_map[metadata_name]):
            id = uuid.uuid7()

            color = sample_metadata_map[metadata_name][value]["color"]

            print(
                f"INSERT INTO metadata (id, metadata_type_id, value, color) VALUES ('{id}', '{metadata_type_id}', '{value}', '{color}');",
                file=f,
            )

            metadata_map[metadata_name][value] = id

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for mi, metadata_name in enumerate(sample_metadata_names):
        for value in sorted(sample_metadata_map[metadata_name]):
            metadata_id = metadata_map[metadata_name][value]
            for sample_id in sorted(
                sample_metadata_map[metadata_name][value]["samples"]
            ):
                id = uuid.uuid7()

                print(
                    f"INSERT INTO sample_metadata (id, sample_id, metadata_id) VALUES ('{id}', '{sample_id}', '{metadata_id}');",
                    file=f,
                )

    print("COMMIT;", file=f)

    if args.technology == "Microarray":
        rma_file = filetypes[0]["file"]

        load_data(
            sample_names,
            "RMA",
            rma_file,
            dataset_id,
            exp_map,
        )

        expr_types = ["RMA"]

        print("BEGIN TRANSACTION;", file=f)

        expr_type_map = {expr_type: uuid.uuid7() for expr_type in expr_types}

        for expr_type in expr_types:
            id = uuid.uuid7()
            print(
                f"INSERT INTO expr_types (id, name) VALUES ('{id}', '{expr_type}');",
                file=f,
            )

        print("COMMIT;", file=f)

        print("BEGIN TRANSACTION;", file=f)

        print(exp_map.keys())

        for dataset_id in sorted(exp_map):
            for probe_id in sorted(exp_map[dataset_id]):
                for gene_symbol in sorted(exp_map[dataset_id][probe_id]):
                    values = exp_map[dataset_id][probe_id][gene_symbol]["RMA"]

                    gene_id = gene_id_map[gene_symbol]

                    binary_data = struct.pack("<" + "f" * len(values), *values)

                    hex_data = binary_data.hex()

                    blob_literal = f"X'{hex_data}'"

                    expr_type_id = expr_type_map["RMA"]

                    print(
                        f"INSERT INTO expr (gene_id, probe_id, expr_type_id, data) VALUES ('{gene_id}', '{probe_id}', '{expr_type_id}', {blob_literal});",
                        file=f,
                    )

                    # for si, sample in enumerate(sample_ids):
                    #     sample_index = sample_db_ids[sample]
                    #     value = values[si]
                    #     print(
                    #         f"INSERT INTO expr (sample_id, gene_id, probe_id, expr_type_id, value) VALUES ({sample_index}, {gene_index}, '{probe_id}', 1, {np.round(value, 4)});",
                    #         file=f,
                    #     )

        print("COMMIT;", file=f)

    else:
        expr_types = set()

        for ft in filetypes:

            counts_file = ft["file"]
            load_data(
                sample_names,
                ft["type"],
                counts_file,
                dataset_id,
                exp_map,
            )

            # tpm_file = args.tpm
            # load_data(
            #     "TPM",
            #     tpm_file,
            #     dataset_id,
            #     exp_map,
            # )

            # vst_file = args.vst
            # load_data(
            #     "VST",
            #     vst_file,
            #     dataset_id,
            #     exp_map,
            # )
            expr_types.add(ft["type"])
            # expr_types.add("TPM")
            # expr_types.add("VST")

        print("BEGIN TRANSACTION;", file=f)

        expr_types = sorted(expr_types)

        expr_type_map = {expr_type: uuid.uuid7() for expr_type in expr_types}

        for expr_type in expr_types:
            id = expr_type_map[expr_type]  # f"{args.technology.lower()}:{uuid.uuid7()}"
            print(
                f"INSERT INTO expr_types (id, name) VALUES ('{id}', '{expr_type}');",
                file=f,
            )

        print("COMMIT;", file=f)

        print("BEGIN TRANSACTION;", file=f)

        print(expr_types)

        for dataset_id in sorted(exp_map):
            for probe_id in sorted(exp_map[dataset_id]):
                for gene_symbol in sorted(exp_map[dataset_id][probe_id]):
                    gene_id = gene_id_map[gene_symbol]

                    for data_type in expr_types:

                        if data_type not in exp_map[dataset_id][probe_id][gene_symbol]:
                            continue

                        expr_type_id = expr_type_map[data_type]

                        values = exp_map[dataset_id][probe_id][gene_symbol][data_type]

                        # unpack as float32
                        binary_data = struct.pack("<" + "f" * len(values), *values)

                        hex_data = binary_data.hex()

                        blob_literal = f"X'{hex_data}'"

                        id = uuid.uuid7()

                        print(
                            f"INSERT INTO expr (id, gene_id, expr_type_id, data) VALUES ('{id}', '{gene_id}', '{expr_type_id}', {blob_literal});",
                            file=f,
                        )

                    # gene_index = gene_db_map[gene_symbol]
                    # t = ", ".join(DATA_TYPES)
                    # print(
                    #     f"INSERT INTO expr (gene_id, {t}) VALUES ({gene_index}, {values});",
                    #     file=f,
                    # )

        print("COMMIT;", file=f)


with open(
    f"../data/modules/gex/{args.species}/{args.technology}/{file_id}.sql", "a"
) as f:
    print("BEGIN TRANSACTION;", file=f)

    for i, id in enumerate(gene_ids):
        gene = official_symbols[id]

        fields = []  # {"name": "id", "value": i + 1}]

        for key, value in gene.items():
            if value != "":
                fields.append({"name": key, "value": value})

        print(
            f"INSERT INTO genes ({', '.join([field['name'] for field in fields])}) VALUES ({', '.join([f'\'{field['value']}\'' for field in fields])});",
            file=f,
        )

    print("COMMIT;", file=f)
