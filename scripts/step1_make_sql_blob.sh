#
# Some microarry datasets
#
species="Human"
technology="Microarray"

platform="HG-U133_Plus_2"
dataset_name="Harvard DFCI Shipp DLBCL"
institution="Harvard DFCI"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/microarray/data/human/other_labs/dlbcl_harvard_shipp/dlbcl/phenotypes_match_rma.txt
rma=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/microarray/data/human/other_labs/dlbcl_harvard_shipp/dlbcl/shipp_dlbcl_rma_approved.tsv
 
python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --platform="${platform}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="RMA,${rma}"


platform="HG-U133_Plus_2"
dataset_name="RDF DLBCL"
institution="RDF"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/microarray/data/human/rdf/hg-u133_plus2/dlbcl/phenotypes.tsv
rma=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/microarray/data/human/rdf/hg-u133_plus2/dlbcl/dlbcl_rma_approved.tsv
 
python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --platform="${platform}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="RMA,${rma}"


#exit(0)

# some single cell

technology="scRNA-seq"
species="Human"

# /ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/
dataset_name="Frontiers B-cells"
institution="RDF"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/mast/phenotypes_ordered.txt
log2=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/mast/mast_all_RK01-07_for_heatmaps_avg_tpm_log2_simple.txt
fc=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/mast/mast_all_RK01_02_03_04_05_06_07_log2fc_simple.txt
 
python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="log2(CPM+1),${log2}" \
    --filetype="log2(FC),${fc}"

#exit(0)
 

technology="RNA-seq"
 

# /ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/
dataset_name="RDF 29 DLBCL Cell Lines"
institution="RDF"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/phenotypes.txt
counts=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/counts_grch37v29_20221014_simple.tsv
tpm=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/tpm_grch37v29_20221014_simple.tsv
vst=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/dlbcl_cell_lines_elodie_29/vst_grch37v29_20221014_simple.tsv

python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="Counts,${counts}" \
    --filetype="TPM,${tpm}" \
    --filetype="VST,${vst}"

 


dataset_name="BCCA Morin DLBCL 230"
institution="BCCA"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/phenotypes_230_renamed.txt
counts="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/counts_dup_grch37_20190507_renamed_230.tsv"
tpm="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/tpm_grch37_20190508_renamed_230.tsv"
vst="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/vst_grch37_20190507_renamed_230.tsv"
 
python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="Counts,${counts}" \
    --filetype="TPM,${tpm}" \
    --filetype="VST,${vst}" \
    --id_col_count=3

 

dataset_name="RDF N/GC/M/DZ/LZ"
institution="RDF"
phenotypes="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/phenotypes.tsv"
counts="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
tpm="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
vst="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"

python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="Counts,${counts}" \
    --filetype="TPM,${tpm}" \
    --filetype="VST,${vst}"


dataset_name="NCI Staudt DLBCL 481"
institution="NCI"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/phenotypes.txt
counts=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/counts_grch38_20190807_renamed.txt
tpm=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/tpm_grch38_20190807_renamed.txt
vst=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/human/other_labs/nci_staudt/dlbcl/transcriptome/grch38/vst_counts_grch38_20190807_renamed.txt

python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="Counts,${counts}" \
    --filetype="TPM,${tpm}" \
    --filetype="VST,${vst}"






# mouse
 
species="Mouse"

dataset_name="Kurosaki Bach2 high/low"
institution="Osaka"
phenotypes=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/phenotypes.tsv
counts=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/counts_grcm38_20241128_simple_cleaned.tsv
tpm=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/tpm_grcm38_20241128_simple_cleaned.tsv
vst=/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/rna_seq/data/mouse/other_labs/kurosaki/bach2_high_low/analysis/vst_grcm38_20241128_simple_cleaned.tsv

python step1_make_gex_sql_blob.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --technology="${technology}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --filetype="Counts,${counts}" \
    --filetype="TPM,${tpm}" \
    --filetype="VST,${vst}"


