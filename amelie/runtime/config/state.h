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
	Var  version;
	Var  directory;
	Var  checkpoint;
	Var  lsn;
	Var  psn;
	Var  read_only;
	// persistent
	Var  repl;
	Var  repl_primary;
	Var  replicas;
	Var  users;
	// stats
	Var  connections;
	Var  sent_bytes;
	Var  recv_bytes;
	Var  writes;
	Var  writes_bytes;
	Var  ops;
	Vars vars;
};

void state_init(State*);
void state_free(State*);
void state_prepare(State*);
void state_open(State*, const char*);
void state_save(State*, const char*);
