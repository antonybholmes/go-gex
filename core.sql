PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE genes (
	id INTEGER PRIMARY KEY ASC,
	hugo_id TEXT NOT NULL DEFAULT '',
	mgi_id TEXT NOT NULL DEFAULT '',	
	ensembl_id TEXT NOT NULL DEFAULT '',
	refseq_id TEXT NOT NULL DEFAULT '',
	ncbi_id TEXT NOT NULL DEFAULT '',
	gene_symbol TEXT NOT NULL DEFAULT '');

CREATE TABLE dataset (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	species TEXT NOT NULL UNIQUE,
	Technology TEXT NOT NULL UNIQUE,
	Platform TEXT NOT NULL UNIQUE DEFAULT '',
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

CREATE TABLE expr_types (
    id INTEGER PRIMARY KEY ASC,
	public_id TEXT UNIQUE NOT NULL UNIQUE,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE expression (
	id INTEGER PRIMARY KEY ASC,
	sample_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	probe_id TEXT NOT NULL DEFAULT '',
	expr_type_id INTEGER NOT NULL,
	value REAL NOT NULL DEFAULT -1,
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(gene_id) REFERENCES genes(id),
	FOREIGN KEY(expr_type_id) REFERENCES expr_types(id));




