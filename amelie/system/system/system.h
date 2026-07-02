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

typedef struct System System;

struct System
{
	Share     share;
	// vm
	Functions functions;
	// cdc
	Cdc       cdc;
	// repl
	Repl      repl;
	// transactions
	Gtrs      gtrs;
	Commit    commit;
	Frontends frontends;
	Backends  backends;
	// db
	Db        db;
	// server
	Servers   servers;
	// runtime control
	RuntimeIf runtime_if;
};

System*
system_create(void);
void system_free(System*);
void system_start(System*, bool);
void system_stop(System*);
void system_main(System*);
