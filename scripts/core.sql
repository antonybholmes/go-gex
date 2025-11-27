PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE genes (
	id TEXT PRIMARY KEY ASC,
	ensembl TEXT NOT NULL DEFAULT '',
	refseq TEXT NOT NULL DEFAULT '',
	ncbi INTEGER NOT NULL DEFAULT 0,
	gene_symbol TEXT NOT NULL DEFAULT '');

CREATE TABLE dataset (
	id TEXT PRIMARY KEY ASC,
	species TEXT NOT NULL,
	technology TEXT NOT NULL,
	platform TEXT NOT NULL,
	institution TEXT NOT NULL,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '');

CREATE TABLE samples (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT '');

-- CREATE TABLE sample_alt_names (
-- 	id INTEGER PRIMARY KEY ASC,
-- 	sample_id INTEGER NOT NULL,
-- 	name TEXT NOT NULL,
-- 	value TEXT NOT NULL,
-- 	FOREIGN KEY(sample_id) REFERENCES samples(id));

CREATE TABLE metadata_types (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	UNIQUE(name));

CREATE TABLE metadata (
	id TEXT PRIMARY KEY ASC,
	metadata_type_id TEXT NOT NULL,
	value TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	color TEXT NOT NULL DEFAULT '',
	UNIQUE(metadata_type_id, value, color),
	FOREIGN KEY(metadata_type_id) REFERENCES metadata_types(id));

CREATE TABLE sample_metadata (
	id TEXT PRIMARY KEY ASC,
	sample_id TEXT NOT NULL,
	metadata_id TEXT NOT NULL,
	UNIQUE(sample_id, metadata_id),
	FOREIGN KEY(sample_id) REFERENCES samples(id),
	FOREIGN KEY(metadata_id) REFERENCES metadata(id));

CREATE TABLE expr_types (
    id TEXT PRIMARY KEY ASC,
    name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT ''
);

-- CREATE TABLE expr (
-- 	id INTEGER PRIMARY KEY ASC,
-- 	sample_id INTEGER NOT NULL,
-- 	gene_id INTEGER NOT NULL,
-- 	probe_id TEXT NOT NULL DEFAULT '',
-- 	expr_type_id INTEGER NOT NULL,
-- 	value REAL NOT NULL DEFAULT 0,
-- 	FOREIGN KEY(sample_id) REFERENCES samples(id),
-- 	FOREIGN KEY(gene_id) REFERENCES genes(id),
-- 	FOREIGN KEY(expr_type_id) REFERENCES expr_types(id));

CREATE TABLE expr (
	id TEXT PRIMARY KEY ASC,
	gene_id TEXT NOT NULL,
	probe_id TEXT NOT NULL DEFAULT '',
	expr_type_id TEXT NOT NULL,
	data BLOB NOT NULL,
	FOREIGN KEY(gene_id) REFERENCES genes(id),
	FOREIGN KEY(expr_type_id) REFERENCES expr_types(id));



