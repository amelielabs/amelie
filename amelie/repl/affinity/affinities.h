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

typedef struct Affinities Affinities;

struct Affinities
{
	List list;
};

void affinities_init(Affinities*);
void affinities_free(Affinities*);
void affinities_open(Affinities*);
void affinities_set(Affinities*, Affinity*);
Affinity*
affinities_find(Affinities*, Str*);
