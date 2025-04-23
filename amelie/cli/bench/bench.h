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

typedef struct BenchIf     BenchIf;
typedef struct BenchWorker BenchWorker;
typedef struct Bench       Bench;

struct BenchIf
{
	void (*create)(Bench*, Client*);
	void (*main)(BenchWorker*, Client*);
};

struct BenchWorker
{
	int    connections;
	bool   shutdown;
	Bench* bench;
	Task   task;
	List   link;
};

struct Bench
{
	BenchIf*   iface;
	atomic_u64 transactions;
	atomic_u64 writes;
	Opt        type;
	Opt        threads;
	Opt        clients;
	Opt        time;
	Opt        scale;
	Opt        batch;
	Opt        init;
	Opt        unlogged;
	Opts       opts;
	Remote*    remote;
	List       list;
};

void bench_init(Bench*, Remote*);
void bench_free(Bench*);
void bench_run(Bench*);
