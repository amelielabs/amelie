#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct Config Config;

struct Config
{
	// main
	Str     version;
	Uuid    uuid;
	Str     timezone;
	bool    daemon;

	// log
	bool    log_enable;
	bool    log_file;
	bool    log_stdout;
	bool    log_stdout_time;
	bool    log_connections;
	bool    log_options;

	// wal
	bool    wal_crc;
	bool    wal_sync_create;
	bool    wal_sync_close;
	bool    wal_sync_write;
	Str     wal_sync_interval;
	bool    wal_service;
	int64_t wal_size;
	int64_t wal_checkpoint;
	bool    wal_rewind;
	int64_t wal_rewind_pos;

	// storage
	bool    storage_crc;
	Str     storage_compression;
	bool    storage_sync;

	// compute
	int64_t compute;

	// replication
	int64_t repl_readahead;
	int64_t repl_reconnect_ms;

	// network
	int64_t recv;
	Str     listen;

	// options
	Opts    opts;
};

void config_init(Config*);
void config_free(Config*);
void config_prepare(Config*);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
