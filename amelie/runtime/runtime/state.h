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

enum
{
	RECOVER_OFF,
	RECOVER_CHECKPOINT,
	RECOVER_WAL,
	RECOVER_REPL
};

struct State
{
	// system
	Opt  version;
	Opt  directory;
	Opt  lsn;
	Opt  tsn;
	Opt  psn;
	Opt  rsn;
	Opt  checkpoint;
	Opt  recover;
	// persistent
	Opt  secret;
	Opt  repl;
	Opt  repl_primary_;
	Opt  replicas;
	Opts opts;
};

void state_init(State*);
void state_free(State*);
void state_prepare(State*);
void state_open(State*, const char*);
void state_save(State*, const char*);
