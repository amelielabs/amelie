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

typedef struct Logger Logger;

struct Logger
{
	bool      enable;
	bool      to_stdout;
	bool      cli;
	bool      cli_lf;
	int       fd;
	Timezone* timezone;
};

void logger_init(Logger*);
void logger_open(Logger*, const char*);
void logger_set_enable(Logger*, bool);
void logger_set_cli(Logger*, bool, bool);
void logger_set_to_stdout(Logger*, bool);
void logger_set_timezone(Logger*, Timezone*);
void logger_close(Logger*);
void logger_write(void*, const char*, const char*, int,
                  const char*, const char*);
