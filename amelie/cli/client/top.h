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

typedef struct TopStats TopStats;
typedef struct Top      Top;

struct TopStats
{
	int64_t  time_us;

	Str      uuid;
	Str      version; 
	int64_t  frontends;
	int64_t  backends;

	// db
	int64_t  schemas;
	int64_t  tables;
	int64_t  secondary_indexes;
	int64_t  views; 

	// process
	int64_t  uptime;
	int64_t  mem_virt;
	int64_t  mem_resident;
	int64_t  mem_shared;
	int64_t  cpu_count;
	int64_t  cpu;
	Buf      cpu_frontends;
	Buf      cpu_backends;

	// net
	int64_t  connections;
	int64_t  sent;
	int64_t  recv;

	// wal
	int64_t  wal_write;
	int64_t  wals;
	int64_t  checkpoint;
	int64_t  lsn;
	int64_t  transactions;
	int64_t  ops;

	// repl
	Buf      repl;
};

struct Top
{
	int       stats_count;
	TopStats* stats[2];
	Json      json;
	Remote*   remote;
};

void top_init(Top*, Remote*);
void top_free(Top*);
void top_run(Top*);
