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

typedef struct Import Import;

struct Import
{
	// stats
	uint64_t    rows;
	uint64_t    errors;
	uint64_t    report_time;
	uint64_t    report_processed;
	uint64_t    report_rows;
	// worker
	List        clients_list;
	MainClient* forward;
	Reader      reader;
	// options
	Opt         batch;
	Opt         clients;
	Opts        opts;
	Main*       main;
};

void import_init(Import*, Main*);
void import_free(Import*);
void import_run(Import*);
