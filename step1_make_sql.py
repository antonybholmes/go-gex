import collections
import re
import sys
import pandas as pd
import numpy as np
from nanoid import generate

file = "/ifs/scratch/cancer/Lab_RDF/ngs/references/hugo/hugo_20240524.tsv"
df_hugo = pd.read_csv(file, sep="\t", header=0, keep_default_na=False)

ensembl_map = {}

for i, gene_id in enumerate(df_hugo["Approved symbol"].values):

    genes = [gene_id] + list(
        filter(
            lambda x: x != "",
            [x.strip() for x in df_hugo["Previous symbols"].values[i].split(",")],
        )
    )

    ensembl = df_hugo["Ensembl gene ID"].values[i]

    for g in genes:
        ensembl_map[g] = ensembl


def get_file_id(name: str) -> str:
    return re.sub(r"[\/ ]+", "_", name.lower())


def load_data(
    gex_type,
    data_type,
    file,
    dataset_name,
    dataset_id,
    samples,
    sample_map,
    exp_map,
    gene_map,
    filter="",
):
    print(dataset_name)

    df = pd.read_csv(file, sep="\t", header=0, index_col=0, keep_default_na=False)

    if filter != "":
        df = df.iloc[:, np.where(df.columns.str.contains(filter, regex=True))[0]]

    for i, gene in enumerate(df.index):
        if gene not in gene_map:
            genes.append({"symbol": gene, "id": ensembl_map.get(gene, "")})
            gene_map[gene] = len(genes)

        gene_id = gene_map[gene]

        for j in range(df.shape[1]):
            sample = df.columns.values[j].split(" ")[0]
            # lets use slash as a delimiter in the name
            sample = sample.replace("|", "/")

            coo = "NA"
            lymphgen = "NA"
            tokens = sample.split("/")

            if len(tokens) > 1:
                coo = tokens[1]

            if len(tokens) > 2:
                lymphgen = tokens[2]

            if sample not in sample_map:
                samples.append(sample)

                sample_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

                sample_map[sample] = {
                    "id": len(samples),
                    "name": sample,
                    "sample_id": sample_id,
                    "dataset_id": dataset_id,
                    "coo": coo,
                    "lymphgen": lymphgen,
                }

            sample_id = sample_map[sample]["sample_id"]

            exp_map[gex_type][dataset_id][sample_id][gene_id][data_type] = df.iloc[i, j]


exp_map = collections.defaultdict(
    lambda: collections.defaultdict(
        lambda: collections.defaultdict(
            lambda: collections.defaultdict(lambda: collections.defaultdict(float))
        )
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

#
# rna-seq
#

samples = []
sample_map = {}

dataset_name = "RDF N/GC/M/DZ/LZ"
file_id = get_file_id(dataset_name)
dataset_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
dataset = {
    "dataset_id": dataset_id,
    "name": dataset_name,
    "institution": "RDF",
    "platform": "RNA-seq",
}


file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
load_data(
    "RNA-seq",
    "counts",
    file,
    dataset_name,
    dataset_id,
    samples,
    sample_map,
    exp_map,
    gene_map,
    "N_|M_|CB_|DZ|LZ",
)
file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
load_data(
    "RNA-seq",
    "tpm_log2",
    file,
    dataset_name,
    dataset_id,
    samples,
    sample_map,
    exp_map,
    gene_map,
    "N_|M_|CB_|DZ|LZ",
)
file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"
load_data(
    "RNA-seq",
    "vst",
    file,
    dataset_name,
    dataset_id,
    samples,
    sample_map,
    exp_map,
    gene_map,
    "N_|M_|CB_|DZ|LZ",
)


num_types = ["counts", "tpm", "vst"]

with open(f"../../data/modules/gex/{file_id}.sql", "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    print(
        f"INSERT INTO dataset (public_id, name, institution, platform) VALUES ('{dataset["dataset_id"]}', '{dataset["name"]}', '{dataset["institution"]}', {dataset["platform"]});",
        file=f,
    )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    for sample in samples:
        sample_info = sample_map[sample]
        print(
            f"INSERT INTO samples (public_id, name, coo, lymphgen) VALUES ('{sample_info["sample_id"]}', '{sample_info["name"]}', '{sample_info["coo"]}', '{sample_info["lymphgen"]}');",
            file=f,
        )

    print("COMMIT;", file=f)

    print("BEGIN TRANSACTION;", file=f)

    # exp_map[gex_type][dataset_id][gene_id][sample_id][data_type]
    type = "RNA-seq"

    for dataset_id in sorted(exp_map[type]):
        for sample_id in sorted(exp_map[type][dataset_id]):
            for gene_id in sorted(exp_map[type][dataset_id][sample_id]):
                values = ", ".join(
                    [
                        str(exp_map[type][dataset_id][sample_id][gene_id][num_type])
                        for num_type in num_types
                    ]
                )

                print(
                    f'INSERT INTO rna_seq (sample_id, gene_id, {", ".join(
                        num_types)}) VALUES ({dataset_id}, {sample_id}, {gene_id}, {values});',
                    file=f,
                )

    print("COMMIT;", file=f)


exit(0)


# dataset_name = "RDF GC"
# datasets.append({"name": dataset_name, "institution": "RDF", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "CB_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "CB_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "CB_"
# )

# dataset_name = "RDF M"
# datasets.append({"name": dataset_name, "institution": "RDF", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "M_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "M_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "M_"
# )

# dataset_name = "RDF DZ"
# datasets.append({"name": dataset_name, "institution": "RDF", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "DZ_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "DZ_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "DZ_"
# )

# dataset_name = "RDF LZ"
# datasets.append({"name": dataset_name, "institution": "RDF", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "LZ_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "LZ_"
# )
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"
# load_data(
#     1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map, "LZ_"
# )


# dataset_name = "RDF 29 Cell lines DLBCL"
# datasets.append({"name": dataset_name, "institution": "RDF", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/counts_grch37v29_20221014_simple.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/tpm_grch37v29_20221014_simple.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/vst_grch37v29_20221014_simple.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)


# # file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
# # load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)


# # load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# dataset_name = "NCI Staudt DLBCL"
# datasets.append({"name": dataset_name, "institution": "NCI", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/counts_grch38_20190807_renamed.txt"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/tpm_grch38_20190807_renamed.txt"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/vst_counts_grch38_20190807_renamed.txt"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)


# dataset_name = "BCCA Morin DLBCL"
# datasets.append({"name": dataset_name, "institution": "BCCA", "platform_id": 1})
# dataset_map[dataset_name] = len(datasets)
# dir = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis"
# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/counts_dup_grch37_20190507_renamed_230.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/tpm_dup_grch37_20190508_renamed_230.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/vst_counts_grch37_20190507_renamed_230.tsv"
# load_data(1, file, dataset_name, dataset_map, samples, sample_map, exp_map, gene_map)


# #
# # microarray
# #

# dataset_name = "Harvard Shipp DLBCL"
# datasets.append({"name": dataset_name, "institution": "Harvard", "platform_id": 2})
# dataset_map[dataset_name] = len(datasets)

# file = "/ifs/scratch/cancer/Lab_RDF/ngs/microarray/data/human/other_labs/dlbcl_harvard_shipp/dlbcl/shipp_dlbcl_rma_approved_max_med_renamed.tsv"
# load_data(
#     2,
#     file,
#     dataset_name,
#     dataset_map,
#     samples,
#     sample_map,
#     exp_map,
#     gene_map,
# )


with open(f"../../data/modules/gex/genes.sql", "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    for gene in genes:
        print(
            f"INSERT INTO genes (gene_id, gene_symbol) VALUES ('{gene["id"]}', '{gene["symbol"]}');",
            file=f,
        )

    print("COMMIT;", file=f)


with open(f"../../data/modules/gex/datasets.sql", "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    for dataset in datasets:

        print(
            f"INSERT INTO datasets (public_id, name, institution, platform) VALUES ('{dataset["dataset_id"]}', '{dataset["name"]}', '{dataset["institution"]}', {dataset["platform"]});",
            file=f,
        )

    print("COMMIT;", file=f)


with open(f"../../data/modules/gex/samples.sql", "w") as f:
    print("BEGIN TRANSACTION;", file=f)

    for sample in samples:
        public_id = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
        print(
            f"INSERT INTO samples (dataset_id, public_id, name, coo, lymphgen) VALUES ({sample["dsid"]}, '{public_id}', '{sample["name"]}', '{sample["coo"]}', '{sample["lymphgen"]}');",
            file=f,
        )

    print("COMMIT;", file=f)


# with open(f"../../data/modules/gex/gex.sql", "w") as f:
#     type = 2
#     num_types = ["rma"]

#     print("BEGIN TRANSACTION;", file=f)

#     for dsid in sorted(exp_map[type]):

#         for sample_id in sorted(exp_map[type][dsid]):
#             for gene_id in sorted(exp_map[type][dsid][sample_id]):
#                 values = ", ".join(
#                     [str(x) for x in exp_map[type][dsid][sample_id][gene_id]]
#                 )

#                 print(
#                     f'INSERT INTO microarray (dataset_id, sample_id, gene_id, {", ".join(
#                         num_types)}) VALUES ({dsid}, {sample_id}, {gene_id}, {values});',
#                     file=f,
#                 )

#     print("COMMIT;", file=f)

#     type = 1
#     num_types = ["counts", "tpm", "vst"]

#     print("BEGIN TRANSACTION;", file=f)

#     for dsid in sorted(exp_map[type]):

#         for sample_id in sorted(exp_map[type][dsid]):
#             for gene_id in sorted(exp_map[type][dsid][sample_id]):
#                 values = ", ".join(
#                     [str(x) for x in exp_map[type][dsid][sample_id][gene_id]]
#                 )

#                 print(
#                     f'INSERT INTO rna_seq (dataset_id, sample_id, gene_id, {", ".join(
#                         num_types)}) VALUES ({dsid}, {sample_id}, {gene_id}, {values});',
#                     file=f,
#                 )

#     print("COMMIT;", file=f)
