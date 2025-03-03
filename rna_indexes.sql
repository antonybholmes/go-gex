CREATE INDEX samples_dataset_name_idx ON samples (name);
CREATE INDEX rna_seq_gene_id_sample_id_idx ON rna_seq (gene_id, sample_id);
 