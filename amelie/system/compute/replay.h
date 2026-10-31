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

typedef struct Compute Compute;
typedef struct Replay  Replay;

struct Replay
{
	Portal   portal;
	Request  req;
	void*    session;
	Buf      buf;
	Compute* compute;
};

void replay_init(Replay*, Compute*);
void replay_free(Replay*);
void replay_run(Replay*, RecordMsg*);
