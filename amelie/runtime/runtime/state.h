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

typedef struct State State;

struct State
{
	// system
	Opt  version;
	Opt  directory;
	Opt  checkpoint;
	Opt  lsn;
	Opt  psn;
	Opt  read_only;
	// persistent
	Opt  repl;
	Opt  repl_primary;
	Opt  replicas;
	Opt  users;
	// stats
	Opt  connections;
	Opt  sent_bytes;
	Opt  recv_bytes;
	Opt  writes;
	Opt  writes_bytes;
	Opt  ops;
	Opts opts;
};

void state_init(State*);
void state_free(State*);
void state_prepare(State*);
void state_open(State*, const char*);
void state_save(State*, const char*);
