PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE datasets (
	id TEXT PRIMARY KEY ASC,
	species TEXT NOT NULL,
	technology TEXT NOT NULL,
	platform TEXT NOT NULL,
	institution TEXT NOT NULL,
	name TEXT NOT NULL UNIQUE,
	path TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '');


 