#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct BenchIf     BenchIf;
typedef struct BenchWorker BenchWorker;
typedef struct Bench       Bench;

struct BenchIf
{
	void (*init)(Bench*, Client*);
	void (*cleanup)(Bench*, Client*);
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
	Var        type;
	Var        threads;
	Var        clients;
	Var        time;
	Var        scale;
	Var        batch;
	Vars       vars;
	Remote*    remote;
	List       list;
};

void bench_init(Bench*, Remote*);
void bench_free(Bench*);
void bench_run(Bench*);
