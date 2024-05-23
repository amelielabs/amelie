#pragma once

//
// sonata.
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
	// log
	Var  log_enable;
	Var  log_to_file;
	Var  log_to_stdout;
	Var  log_connections;
	Var  log_query;
	// server
	Var  tls_ca;
	Var  tls_cert;
	Var  tls_key;
	Var  listen;
	// cluster
	Var  frontends;
	Var  shards;
	// wal
	Var  wal;
	Var  wal_rotate_wm;
	Var  wal_sync_on_rotate;
	Var  wal_sync_on_write;
	// state
	Var  lsn;
	Var  ssn;
	Var  checkpoint;
	Var  state_shards;
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
void config_set(Config*, Str*);
void config_open(Config*, const char*);
void config_save(Config*, const char*);
void config_print(Config*);
Buf* config_list(Config*);
Var* config_find(Config*, Str*);
