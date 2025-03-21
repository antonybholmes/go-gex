platform="RNA-seq"
species="Human"

dataset_name="BCCA Morin DLBCL 230"
institution="BCCA"
phenotypes=/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/phenotypes_230_renamed.txt
counts="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/counts_dup_grch37_20190507_renamed_230.tsv"
tpm="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/tpm_grch37_20190508_renamed_230.tsv"
vst="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/other_labs/BC_morin/dlbcl_ega_EGAD00001003783/324/grch37/analysis/vst_grch37_20190507_renamed_230.tsv"
id_col_count=3
python make_gex_sql.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --platform="${platform}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --counts="${counts}" \
    --tpm="${tpm}" \
    --vst="${vst}" \
    --id_col_count=3


dataset_name="RDF N/GC/M/DZ/LZ"
institution="RDF"
phenotypes="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/phenotypes.tsv"
counts="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_counts_restricted_gencode_grch38_20180724_simple.tsv"
tpm="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/n_gc_m_lz_dz_tpm_restricted_gencode_grch38_20180724_simple.tsv"
vst="/ifs/scratch/cancer/Lab_RDF/ngs/rna_seq/data/human/rdf/n_m_gc_lz_dz/vst_n_gc_m_lz_dz_restricted_gencode_grch38_20180724.txt"

python make_gex_sql.py \
    --name="${dataset_name}" \
    --institution="${institution}" \
    --platform="${platform}" \
    --species="${species}" \
    --phenotypes="${phenotypes}" \
    --counts="${counts}" \
    --tpm="${tpm}" \
    --vst="${vst}"
 