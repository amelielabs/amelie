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

typedef struct ImportClient ImportClient;
typedef struct Import       Import;

struct ImportClient
{
	Client* client;
	int     pending;
	List    link;
};

struct Import
{
	// stats
	uint64_t      rows;
	uint64_t      errors;
	uint64_t      report_time;
	uint64_t      report_processed;
	uint64_t      report_rows;
	// worker
	List          clients_list;
	ImportClient* forward;
	Reader        reader;
	// table
	Str           path;
	Str           content_type;
	// options
	Opt           format;
	Opt           batch;
	Opt           clients;
	Opts          opts;
	Remote*       remote;
};

void import_init(Import*, Remote*);
void import_free(Import*);
void import_run(Import*, int, char**);
