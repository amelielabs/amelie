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

typedef struct Players Players;

struct Players
{
	Player*    player;
	int        player_count;
	uint64_t   player_id;
	int        rr;
	PlayerSync sync;
};

Players*
players_allocate();
void players_free(Players*);
void players_start(Players*, Frontends*);
void players_stop(Players*);
void players_sync(Players*);
void players_execute(Players*, RecordMsg*);
