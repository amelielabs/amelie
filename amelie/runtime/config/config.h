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
	Opt  uuid;
	Opt  timezone;
	Opt  format;
	// log
	Opt  log_enable;
	Opt  log_to_file;
	Opt  log_to_stdout;
	Opt  log_connections;
	Opt  log_options;
	// server
	Opt  tls_capath;
	Opt  tls_ca;
	Opt  tls_cert;
	Opt  tls_key;
	Opt  listen;
	// limits
	Opt  limit_send;
	Opt  limit_recv;
	Opt  limit_write;
	// io and compute
	Opt  frontends;
	Opt  backends;
	// wal
	Opt  wal_worker;
	Opt  wal_crc;
	Opt  wal_sync_on_create;
	Opt  wal_sync_on_close;
	Opt  wal_sync_on_write;
	Opt  wal_sync_interval;
	Opt  wal_size;
	Opt  wal_truncate;
	// replication
	Opt  repl_readahead;
	Opt  repl_reconnect_ms;
	// checkpoint
	Opt  checkpoint_interval;
	Opt  checkpoint_workers;
	Opts opts;
};

void config_init(Config*);
void config_free(Config*);
void config_prepare(Config*);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
