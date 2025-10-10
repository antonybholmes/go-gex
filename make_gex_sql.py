import collections
import re
import sys
import pandas as pd
import numpy as np

# from nanoid import generate
import uuid_utils as uuid
import argparse


DATA_TYPES = ["Counts", "TPM", "VST"]


def get_file_id(name: str) -> str:
    return re.sub(r"[\/ ]+", "_", name.lower())


def load_sample_data(df: pd.DataFrame, num_id_cols: int):

    id_names = df.columns.values[0:num_id_cols]
    sample_metadata_names = df.columns.values[num_id_cols:]

    sample_id_map = collections.defaultdict(lambda: collections.defaultdict(str))
    sample_data_map = collections.defaultdict(lambda: collections.defaultdict(str))

    sample_ids = df.iloc[:, 0].values

    for i, row in df.iterrows():
        values = row.astype(str)

        ids = values[0:num_id_cols]
        sample_id = ids[0]
        alt_id_names = id_names  # [1:]
        alt_ids = ids  # [1:]

        for name, alt_id in zip(alt_id_names, alt_ids):
            # here name is the column name, alt_id is the value
            # e.g. if a sample has multiple ids, e.g. GEO and SRA
            # then name is "GEO" and alt_id is "GSMxxxx" or
            # name is "SRA" and alt_id is "SRRxxxx"
            sample_id_map[sample_id][name] = alt_id

        values = values.astype(str)
        values = values[num_id_cols:]

        for name, value in zip(sample_metadata_names, values):
            if value != "":
                sample_data_map[sample_id][name] = value

    return [
        sample_ids,
        alt_id_names,
        sample_id_map,
        sample_metadata_names,
        sample_data_map,
    ]


def load_data(
    data_type,
    file,
    dataset_id,
    exp_map,
    filter="",
):
    print(file)

    df = pd.read_csv(file, sep="\t", header=0, index_col=0, keep_default_na=False)

    if data_type == "rma":
        probes = df.index.values
        genes = df.iloc[:, 0].values
        df = df.iloc[:, 1:]
    else:
        probes = df.index.values
        genes = df.index.values

    if filter != "":
        df = df.iloc[:, np.where(df.columns.str.contains(filter, regex=True))[0]]

    # print(df.shape)
    # exit(0)

    for i, probe in enumerate(probes):
        gene = genes[i]

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
parser.add_argument("--counts", type=str, help="Counts file")
parser.add_argument("--tpm", type=str, help="TPM file")
parser.add_argument("--vst", type=str, help="VST file")
parser.add_argument("--rma", type=str, help="RMA file")
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


#
# Read gene symbols for matching
#

official_symbols = {}

gene_ids = []
gene_id_map = {}
prev_gene_id_map = {}
alias_gene_id_map = {}
gene_db_map = {}

if args.species == "Mouse":
    file = "/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/references/mgi/mgi_entrez_ensembl_gene_list_20240531.tsv"
    df_mgi = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

    for i, gene_symbol in enumerate(df_mgi["gene_symbol"].values):

        mgi = df_mgi["mgi"].values[i]
        ensembl = df_mgi["ensembl"].values[i]
        refseq = df_mgi["refseq"].values[i].replace(" ", "")
        ncbi = df_mgi["entrez"].values[i].replace(" ", "")

        official_symbols[mgi] = {
            "hugo": "",
            "mgi": mgi,
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
        gene_db_map[mgi] = index
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
        ensembl = df_hugo["Ensembl gene ID"].values[i]
        refseq = df_hugo["RefSeq IDs"].values[i].replace(" ", "")
        ncbi = df_hugo["NCBI Gene ID"].values[i].replace(" ", "")

        official_symbols[hugo] = {
            "hugo": hugo,
            "mgi": "",
            "gene_symbol": gene_symbol,
            "ensembl": ensembl,
            "refseq": refseq,
            "ncbi": ncbi,
        }

        gene_id_map[hugo] = hugo
        gene_id_map[gene_symbol] = hugo
        gene_id_map[refseq] = hugo
        gene_id_map[ncbi] = hugo

        for g in [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")]:
            prev_gene_id_map[g] = hugo

        for g in [x.strip() for x in df_hugo["Alias symbols"].values[i].split(",")]:
            alias_gene_id_map[g] = hugo

        index = i + 1
        gene_db_map[hugo] = index
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


sample_ids, alt_id_names, sample_id_map, sample_metadata_names, sample_data_map = (
    load_sample_data(df_samples, args.id_col_count)
)

# print(sample_id_map)
# print(sample_data_map)


# samples = []
# sample_map = {}

dataset_name = args.name
file_id = get_file_id(dataset_name)
dataset_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
dataset = {
    "dataset_id": dataset_id,
    "name": dataset_name,
    "institution": args.institution,
    "technology": args.technology,
    "platform": args.platform,
    "species": args.species,
}

#
# Write sql
#

with open(f"data/modules/gex/{args.species}/{args.technology}/{file_id}.sql", "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    print(
        f"INSERT INTO dataset (public_id, name, species, institution, technology, platform) VALUES ('{dataset['dataset_id']}', '{dataset['name']}', '{dataset['species']}', '{dataset['institution']}', '{dataset['technology']}', '{dataset['platform']}');",
        file=f,
    )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for i, sample in enumerate(sample_ids):
        public_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
        print(
            f"INSERT INTO samples (public_id, name) VALUES ('{public_id}', '{sample}');",
            file=f,
        )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    sample_db_ids = {sample: i + 1 for i, sample in enumerate(sample_ids)}

    for i, sample in enumerate(sample_ids):
        for name in alt_id_names:
            value = sample_id_map[sample][name]

            print(
                f"INSERT INTO sample_alt_names (sample_id, name, value) VALUES ({i + 1}, '{name}', '{value}');",
                file=f,
            )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for si, sample in enumerate(sample_ids):
        for name in sample_metadata_names:
            value = sample_data_map[sample][name]

            print(
                f"INSERT INTO sample_metadata (sample_id, name, value) VALUES ({si + 1}, '{name}', '{value}');",
                file=f,
            )

    print("COMMIT;", file=f)

    if args.technology == "Microarray":
        rma_file = args.rma

        load_data(
            "RMA",
            rma_file,
            dataset_id,
            exp_map,
        )

        expr_types = set()
        expr_types.add("RMA")

        print("BEGIN TRANSACTION;", file=f)

        expr_type_map = {i: expr_type for i, expr_type in enumerate(sorted(expr_types))}

        for i, expr_type in enumerate(sorted(expr_types)):
            public_id = uuid.uuid7()
            print(
                f"INSERT INTO expr_types (id, public_id, name) VALUES ({i + 1}, '{public_id}', '{expr_type[1]}');",
                file=f,
            )

        print("COMMIT;", file=f)

        print("BEGIN TRANSACTION;", file=f)

        for dataset_id in sorted(exp_map):
            for probe_id in sorted(exp_map[dataset_id]):
                for gene_symbol in sorted(exp_map[dataset_id][probe_id]):
                    values = exp_map[dataset_id][probe_id][gene_symbol]["RMA"]

                    gene_index = gene_db_map[gene_symbol]

                    for si, sample in enumerate(sample_ids):
                        sample_index = sample_db_ids[sample]
                        value = values[si]
                        print(
                            f"INSERT INTO expression (sample_id, gene_id, probe_id, expr_type_id, value) VALUES ({sample_index}, {gene_index}, '{probe_id}', 1, {np.round(value, 4)});",
                            file=f,
                        )

        print("COMMIT;", file=f)

    else:
        expr_types = set()
        counts_file = args.counts
        load_data(
            "Counts",
            counts_file,
            dataset_id,
            exp_map,
        )

        tpm_file = args.tpm
        load_data(
            "TPM",
            tpm_file,
            dataset_id,
            exp_map,
        )

        vst_file = args.vst
        load_data(
            "VST",
            vst_file,
            dataset_id,
            exp_map,
        )
        expr_types.add("Counts")
        expr_types.add("TPM")
        expr_types.add("VST")

        print("BEGIN TRANSACTION;", file=f)

        expr_type_map = {
            expr_type: i + 1 for i, expr_type in enumerate(sorted(expr_types))
        }

        for i, expr_type in enumerate(sorted(expr_types)):
            public_id = uuid.uuid7()
            print(
                f"INSERT INTO expr_types (id, public_id, name) VALUES ({i + 1}, '{public_id}', '{expr_type}');",
                file=f,
            )

        print("COMMIT;", file=f)

        print("BEGIN TRANSACTION;", file=f)

        for dataset_id in sorted(exp_map):
            for probe_id in sorted(exp_map[dataset_id]):
                for gene_symbol in sorted(exp_map[dataset_id][probe_id]):
                    gene_index = gene_db_map[gene_symbol]

                    for data_type in DATA_TYPES:
                        if data_type not in exp_map[dataset_id][probe_id][gene_symbol]:
                            continue

                        expr_type_id = expr_type_map[data_type]

                        values = exp_map[dataset_id][probe_id][gene_symbol][data_type]

                        for si, sample in enumerate(sample_ids):
                            sample_index = sample_db_ids[sample]
                            value = values[si]
                            print(
                                f"INSERT INTO expression (sample_id, gene_id, expr_type_id, value) VALUES ({sample_index}, {gene_index}, {expr_type_id}, {np.round(value, 4)});",
                                file=f,
                            )

                    # gene_index = gene_db_map[gene_symbol]
                    # t = ", ".join(DATA_TYPES)
                    # print(
                    #     f"INSERT INTO expression (gene_id, {t}) VALUES ({gene_index}, {values});",
                    #     file=f,
                    # )

        print("COMMIT;", file=f)


with open(
    f"../../data/modules/gex/{args.species}/{args.technology}/{file_id}.sql", "a"
) as f:
    print("BEGIN TRANSACTION;", file=f)

    if args.species == "Mouse":
        for i, id in enumerate(gene_ids):
            gene = official_symbols[id]

            print(
                f"INSERT INTO genes (id, mgi_id, ensembl_id, refseq_id, ncbi_id, gene_symbol) VALUES ({i + 1}, '{gene['mgi']}', '{gene['ensembl']}', '{gene['refseq']}', '{gene['ncbi']}', '{gene['gene_symbol']}');",
                file=f,
            )
    else:
        for i, id in enumerate(gene_ids):
            gene = official_symbols[id]

            print(
                f"INSERT INTO genes (id, hugo_id, ensembl_id, refseq_id, ncbi_id, gene_symbol) VALUES ({i + 1}, '{gene['hugo']}', '{gene['ensembl']}', '{gene['refseq']}', '{gene['ncbi']}', '{gene['gene_symbol']}');",
                file=f,
            )

    print("COMMIT;", file=f)
