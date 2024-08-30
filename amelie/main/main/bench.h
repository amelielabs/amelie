#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct BenchWorker BenchWorker;
typedef struct Bench       Bench;

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
	atomic_u64 transactions;
	int        opt_threads;
	int        opt_connections;
	int        opt_duration;
	int        opt_batch;
	Remote*    remote;
	List       list;
};

void bench_init(Bench*);
void bench_free(Bench*);
void bench_set_threads(Bench*, int);
void bench_set_connections(Bench*, int);
void bench_set_duration(Bench*, int);
void bench_set_remote(Bench*, Remote*);
void bench_run(Bench*);
