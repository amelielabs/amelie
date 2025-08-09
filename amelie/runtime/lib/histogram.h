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

typedef struct Histogram Histogram;

#define HISTOGRAM_MAX 155

struct Histogram
{
	uint64_t min, max, sum;
	uint64_t buckets[HISTOGRAM_MAX];
	uint64_t count;
};

void histogram_init(Histogram*);
void histogram_add(Histogram*, uint64_t);
void histogram_print(Histogram*);
