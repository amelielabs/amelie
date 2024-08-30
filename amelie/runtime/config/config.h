#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Config Config;

struct Config
{
	// main
	Var  version;
	Var  uuid;
	Var  directory;
	Var  timezone;
	Var  timezone_default;
	// log
	Var  log_enable;
	Var  log_to_file;
	Var  log_to_stdout;
	Var  log_connections;
	// server
	Var  listen;
	// limits
	Var  limit_send;
	Var  limit_recv;
	Var  limit_write;
	// cluster
	Var  frontends;
	Var  backends;
	// wal
	Var  wal;
	Var  wal_rotate_wm;
	Var  wal_sync_on_rotate;
	Var  wal_sync_on_write;
	// replication
	Var  repl;
	Var  repl_primary;
	Var  repl_reconnect_ms;
	// checkpoint
	Var  checkpoint_interval;
	Var  checkpoint_workers;
	Var  checkpoint;
	// state
	Var  read_only;
	Var  lsn;
	Var  psn;
	// state persistent
	Var  nodes;
	Var  replicas;
	Var  users;
	// testing
	Var  test_bool;
	Var  test_int;
	Var  test_string;
	Var  test_data;
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
void config_set(Config*, Str*, bool);
void config_set_argv(Config*, int, char**);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
void config_print(Config*);
Buf* config_list(Config*, ConfigLocal*);
Var* config_find(Config*, Str*);
