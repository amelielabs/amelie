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

typedef struct Replays Replays;

struct Replays
{
	Replay*    replay;
	int        replay_count;
	uint64_t   replay_id;
	int        rr;
	ReplaySync sync;
};

Replays*
replays_allocate();
void replays_free(Replays*);
void replays_start(Replays*, Frontends*);
void replays_stop(Replays*);
void replays_sync(Replays*);
void replays_execute(Replays*, RecordMsg*);
