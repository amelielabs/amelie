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

typedef struct Timezones Timezones;

struct Timezones
{
	Hashtable ht;
	Timezone* system;
};

void timezones_init(Timezones*);
void timezones_free(Timezones*);
void timezones_open(Timezones*);
Timezone*
timezones_find(Timezones*, Str*);
