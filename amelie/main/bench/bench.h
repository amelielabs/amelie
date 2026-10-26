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
typedef struct BenchClient BenchClient;
typedef struct Bench       Bench;

struct BenchIf
{
	void (*deploy)(BenchClient*);
	void (*main)(BenchClient*);
};

struct BenchClient
{
	Client*  client;
	Endpoint endpoint;
	Bench*   bench;
};

struct Bench
{
	bool         shutdown;
	uint64_t     transactions;
	uint64_t     writes;
	BenchClient* clients;
	int          clients_count;
	Histogram    histogram;
	BenchIf*     iface;
	Opt          opt_type;
	Opt          opt_users;
	Opt          opt_clients;
	Opt          opt_duration;
	Opt          opt_scale;
	Opt          opt_batch;
	Opt          opt_histogram;
	Opts         opts;
	Main*        main;
};

void bench_init(Bench*, Main*);
void bench_free(Bench*);
void bench_run(Bench*);
