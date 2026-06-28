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

typedef struct MatchingTop MatchingTop;
typedef struct Matching    Matching;

struct MatchingTop
{
	float    distance;
	uint32_t row;
	Flat*    flat;
	Heap*    heap;
};

struct Matching 
{
	Store       store;
	Columns*    columns;
	Heap*       heap;
	Flat*       flat;
	int         top_count;
	MatchingTop top[] cache_line_aligned;
};

Matching*
matching_create(Columns*, Heap*, Flat*, int);
void matching_execute(Matching*, const float*__restrict);
void matching_merge(Value*, Value**, int);
