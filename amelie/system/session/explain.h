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

typedef struct Explain Explain;

struct Explain
{
	uint64_t time_run_us;
	uint64_t time_commit_us;
};

void explain_init(Explain*);
void explain_reset(Explain*);
void explain(Explain*, Program*, Dtr*, Content*, bool);

static inline void
explain_start(uint64_t* metric)
{
	time_start(metric);
}

static inline void
explain_end(uint64_t* metric)
{
	time_end(metric);
}
