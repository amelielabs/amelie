#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Config Config;

struct Config
{
	// main
	Var  version;
	Var  uuid;
	Var  directory;
	Var  backup;
	// log
	Var  log_enable;
	Var  log_to_file;
	Var  log_to_stdout;
	Var  log_connections;
	Var  log_query;
	// tls
	Var  tls_ca;
	Var  tls_cert;
	Var  tls_key;
	// server
	Var  listen;
	// engine
	Var  engine_workers;
	Var  engine_partition;
	Var  engine_split;
	// wal
	Var  wal_rotate_wm;
	Var  wal_sync_on_rotate;
	Var  wal_sync_on_write;
	// repl
	Var  repl_enable;
	Var  repl_reconnect_ms;
	Var  repl_primary;
	Var  repl_role;
	// system
	Var  lsn;
	Var  psn;
	Var  read_only;
	// state
	Var  users;
	Var  nodes;
	Var  catalog;
	Var  catalog_snapshot;
	// testing
	Var  test_bool;
	Var  test_int;
	Var  test_string;
	Var  test_data;
	Var  error_engine_merger_1;
	Var  error_engine_merger_2;
	Var  error_engine_merger_3;
	Var  error_engine_merger_4;
	List list;
	List list_persistent;
	int  count;
	int  count_visible;
	int  count_config;
	int  count_persistent;
};

void config_init(Config*);
void config_free(Config*);
void config_prepare(Config*);
void config_open(Config*, const char*);
void config_create(Config*, const char*);
void config_set(Config*, bool, Str*);
void config_set_data(Config*, bool, uint8_t**);
void config_copy(Config*, Config*);
void config_print(Config*);
Var* config_find(Config*, Str*);
Buf* config_list(Config*);
Buf* config_list_default(Config*);
Buf* config_list_persistent(Config*);
