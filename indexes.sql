CREATE INDEX samples_dataset_name_idx ON samples (name);
-- CREATE INDEX expression_gene_id_sample_id_idx ON expression (gene_id);

CREATE INDEX expr_type_public_id_idx ON expr_types(public_id);
 
CREATE INDEX expression_sample_type_id_idx ON expression(sample_id, expr_type_id);
CREATE INDEX expression_gene_type_id_idx ON expression(gene_id, expr_type_id);

CREATE INDEX metadata_name_idx ON metadata (name);
CREATE INDEX metadata_public_id_idx ON metadata (public_id);

CREATE INDEX genes_hugo_idx ON genes (hugo);
CREATE INDEX genes_ensembl_idx ON genes (ensembl);
CREATE INDEX genes_refseq_idx ON genes (refseq);
CREATE INDEX genes_gene_symbol_idx ON genes (gene_symbol);
