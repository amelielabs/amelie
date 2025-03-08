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

typedef struct Cli    Cli;
typedef struct CliCmd CliCmd;

typedef enum
{
	CLI_ERROR,
	CLI_RUN,
	CLI_COMPLETE
} CliRc;

struct CliCmd
{
	void       (*function)(Cli*, int, char**);
	const char*  name;
	const char*  description;
};

struct Cli
{
	Home     home;
	Instance instance;
	Task     task;
};

void  cli_init(Cli*);
void  cli_free(Cli*);
CliRc cli_start(Cli*, int, char**);
void  cli_stop(Cli*);
