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
	int32_t  score;
	uint32_t row;
	Flat*    flat;
	Heap*    heap;
};

struct Matching 
{
	Columns*    columns;
	Heap*       heap;
	Flat*       flat;
	int         top_count;
	MatchingTop top[] cache_line_aligned;
};

Matching*
matching_create(Columns*, Heap*, Flat*, int);
void matching_free(Matching*);
void matching_execute(Matching*, const float*__restrict);
void matching_result(Matching*, Value*);
