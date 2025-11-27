CREATE TABLE expr (
	id TEXT PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	probe_id TEXT NOT NULL DEFAULT '',
	expr_type TEXT NOT NULL DEFAULT 'counts',
	value REAL NOT NULL DEFAULT -1,
	UNIQUE (sample_id, gene_id, expr_type),
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(gene_id) REFERENCES genes(id));



