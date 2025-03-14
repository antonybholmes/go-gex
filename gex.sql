PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE datasets (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	species TEXT NOT NULL,
	platform INTEGER NOT NULL,
	institution TEXT NOT NULL,
	name TEXT NOT NULL UNIQUE,
	path TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '');


 