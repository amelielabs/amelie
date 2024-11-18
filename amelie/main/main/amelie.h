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

typedef struct Amelie Amelie;

typedef enum
{
	AMELIE_ERROR,
	AMELIE_RUN,
	AMELIE_COMPLETE
} AmelieRc;

struct Amelie
{
	Home home;
	Main main;
	Task task;
};

void     amelie_init(Amelie*);
void     amelie_free(Amelie*);
AmelieRc amelie_start(Amelie*, int, char**);
void     amelie_stop(Amelie*);
