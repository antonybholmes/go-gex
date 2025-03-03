PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE genes (
	id INTEGER PRIMARY KEY ASC,
	gene_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL);
CREATE INDEX genes_gene_id_idx ON genes (gene_id);
CREATE INDEX genes_gene_symbol_idx ON genes (gene_symbol);

CREATE TABLE platforms (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE);
CREATE INDEX platforms_name_idx ON platforms (name);

INSERT INTO platforms (public_id, name) VALUES ('2j2i4tv0jbw3','RNA-seq');
INSERT INTO platforms (public_id, name) VALUES ('aj1qgz5ghfx6', 'Microarray');

CREATE TABLE gex_value_types (
	id INTEGER PRIMARY KEY ASC,
	platform_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
	FOREIGN KEY(platform_id) REFERENCES platforms(id));

INSERT INTO gex_value_types (platform_id, name) VALUES (1, 'Counts');
INSERT INTO gex_value_types (platform_id, name) VALUES (1, 'TPM');
INSERT INTO gex_value_types (platform_id, name) VALUES (1, 'VST');
INSERT INTO gex_value_types (platform_id, name) VALUES (2, 'RMA');

CREATE TABLE datasets (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	platform_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
	institution TEXT NOT NULL,
	notes TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(platform_id) REFERENCES platforms(id));


CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	platform TEXT NOT NULL UNIQUE,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	institution TEXT NOT NULL,
	notes TEXT NOT NULL DEFAULT '');


	
CREATE TABLE samples (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	coo TEXT NOT NULL DEFAULT 'NA',
	lymphgen TEXT NOT NULL DEFAULT 'NA',
	notes TEXT NOT NULL DEFAULT '');
CREATE INDEX samples_dataset_id_name_idx ON samples (dataset_id, name);
 
CREATE TABLE rna_seq (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	counts INTEGER NOT NULL DEFAULT -1,
	tpm REAL NOT NULL DEFAUlT -1,
	vst REAL NOT NULL DEFAUlT -1,
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(gene_id) REFERENCES genes(id));
CREATE INDEX rna_seq_gene_id_sample_id_idx ON rna_seq (sample_id, gene_id);
 
CREATE TABLE microarray (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	rma REAL NOT NULL DEFAUlT -1,
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(gene_id) REFERENCES genes(id));
CREATE INDEX microarray_gene_id_sample_id_idx ON microarray (sample_id, gene_id);
 
