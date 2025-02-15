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
	Var  uuid;
	Var  timezone;
	Var  format;
	Var  shutdown;
	// log
	Var  log_enable;
	Var  log_to_file;
	Var  log_to_stdout;
	Var  log_connections;
	Var  log_options;
	// server
	Var  tls_capath;
	Var  tls_ca;
	Var  tls_cert;
	Var  tls_key;
	Var  listen;
	// limits
	Var  limit_send;
	Var  limit_recv;
	Var  limit_write;
	// io and compute
	Var  frontends;
	Var  backends;
	// wal
	Var  wal_worker;
	Var  wal_sync_on_rotate;
	Var  wal_sync_on_shutdown;
	Var  wal_sync_on_write;
	Var  wal_size;
	// replication
	Var  repl_reconnect_ms;
	// checkpoint
	Var  checkpoint_interval;
	Var  checkpoint_workers;
	Vars vars;
};

void config_init(Config*);
void config_free(Config*);
void config_prepare(Config*);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
