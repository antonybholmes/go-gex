PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE genes (
	id INTEGER PRIMARY KEY ASC,
	hugo_id TEXT NOT NULL,	
	ensembl_id TEXT NOT NULL,
	refseq_id TEXT NOT NULL,
	ncbi_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL);

CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	species TEXT NOT NULL UNIQUE,
	platform TEXT NOT NULL UNIQUE,
	institution TEXT NOT NULL,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT '');

CREATE TABLE samples (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT '');

CREATE TABLE sample_alt_names (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	value TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(sample_id) REFERENCES samples(id));

CREATE TABLE sample_metadata (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	value TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(sample_id) REFERENCES samples(id));
 
 
CREATE TABLE expression (
	id INTEGER PRIMARY KEY ASC,
	gene_id INTEGER NOT NULL,
	counts INTEGER NOT NULL DEFAULT -1,
	tpm REAL NOT NULL DEFAUlT -1,
	vst REAL NOT NULL DEFAUlT -1,
	FOREIGN KEY(gene_id) REFERENCES genes(id));



