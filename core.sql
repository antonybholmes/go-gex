PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE genes (
	id INTEGER PRIMARY KEY ASC,
	hugo TEXT,
	mgi TEXT,
	ensembl TEXT,
	refseq TEXT,
	ncbi INTEGER,
	gene_symbol TEXT NOT NULL);

CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL,
	species TEXT NOT NULL,
	technology TEXT NOT NULL,
	platform TEXT NOT NULL,
	institution TEXT NOT NULL,
	name TEXT NOT NULL,
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
	value TEXT NOT NULL,
	FOREIGN KEY(sample_id) REFERENCES samples(id));

CREATE TABLE sample_metadata (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	name TEXT NOT NULL,
	value TEXT NOT NULL,
	FOREIGN KEY(sample_id) REFERENCES samples(id));

CREATE TABLE expr_types (
    id INTEGER PRIMARY KEY ASC,
	public_id TEXT UNIQUE NOT NULL UNIQUE,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE expression (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	probe_id TEXT,
	expr_type_id INTEGER NOT NULL,
	value REAL NOT NULL DEFAULT 0,
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(gene_id) REFERENCES genes(id),
	FOREIGN KEY(expr_type_id) REFERENCES expr_types(id));




