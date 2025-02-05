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
	Var  version;
	Var  uuid;
	Var  directory;
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
	// cluster
	Var  hosts;
	Var  nodes;
	// wal
	Var  wal_size;
	Var  wal_sync_on_rotate;
	Var  wal_sync_on_write;
	// replication
	Var  repl_reconnect_ms;
	// checkpoint
	Var  checkpoint_interval;
	Var  checkpoint_workers;
	Var  checkpoint;
	// system
	Var  read_only;
	Var  lsn;
	Var  psn;
	Vars vars;
};

void config_init(Config*);
void config_free(Config*);
void config_prepare(Config*);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
