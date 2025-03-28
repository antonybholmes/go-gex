technology="RNA-seq"
species="Human"

dataset_name="BCCA Morin DLBCL 230"
institution="BCCA"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/phenotypes_230_renamed.txt
counts="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/counts_dup_grch37_20190507_renamed_230.tsv"
tpm="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/tpm_grch37_20190508_renamed_230.tsv"
vst="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/vst_grch37_20190507_renamed_230.tsv"
 
# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --counts="${counts}" \
#     --tpm="${tpm}" \
#     --vst="${vst}" \
#     --id_col_count=3


dataset_name="RDF N/GC/M/DZ/LZ"
institution="RDF"
phenotypes="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/phenotypes.tsv"
counts="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
tpm="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
vst="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"

# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --counts="${counts}" \
#     --tpm="${tpm}" \
#     --vst="${vst}"


dataset_name="NCI Staudt DLBCL 481"
institution="NCI"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/phenotypes.txt
counts=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/counts_grch38_20190807_renamed.txt
tpm=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/tpm_grch38_20190807_renamed.txt
vst=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/vst_counts_grch38_20190807_renamed.txt

# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --counts="${counts}" \
#     --tpm="${tpm}" \
#     --vst="${vst}"


dataset_name="RDF 29 DLBCL Cell Lines"
institution="RDF"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/phenotypes.txt
counts=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/counts_grch37v29_20221014_simple.tsv
tpm=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/tpm_grch37v29_20221014_simple.tsv
vst=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/vst_grch37v29_20221014_simple.tsv

# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --counts="${counts}" \
#     --tpm="${tpm}" \
#     --vst="${vst}"


# mouse
 
species="Mouse"

dataset_name="Kurosaki Bach2 high/low"
institution="Osaka"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/counts_grcm38_20241128_simple_cleaned.tsv
counts=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/counts_grch37v29_20221014_simple.tsv
tpm=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/tpm_grcm38_20241128_simple_cleaned.tsv
vst=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/vst_grcm38_20241128_simple_cleaned.tsv

python make_gex_sql.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --counts="${counts}" \
    --tpm="${tpm}" \
    --vst="${vst}"
 

#
# Some microarry datasets
#
species="Human"
technology="Microarray"

platform="HG-U133_Plus_2"
dataset_name="Harvard DFCI Shipp DLBCL"
institution="Harvard DFCI"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/microarray/data/human/other_labs/dlbcl_harvard_shipp/dlbcl/phenotypes_match_rma.txt
rma=/ifs/scratch/cancer/Lab_RDF/ngs/microarray/data/human/other_labs/dlbcl_harvard_shipp/dlbcl/shipp_dlbcl_rma_approved.tsv
 
# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --platform="${platform}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --rma="${rma}"


platform="HG-U133_Plus_2"
dataset_name="RDF DLBCL"
institution="RDF"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/microarray/data/human/rdf/hg-u133_plus2/dlbcl/phenotypes.tsv
rma=/ifs/scratch/cancer/Lab_RDF/ngs/microarray/data/human/rdf/hg-u133_plus2/dlbcl/dlbcl_rma_approved.tsv
 
# python make_gex_sql.py \
#     --name="${dataset_name}" \
#     --institution="${institution}" \
#     --technology="${technology}" \
#     --platform="${platform}" \
#     --species="${species}" \
#     --phenotypes="${phenotypes}" \
#     --rma="${rma}"
