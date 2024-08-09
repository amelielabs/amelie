#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Explain Explain;

struct Explain
{
	uint64_t time_run_us;
	uint64_t time_commit_us;
};

void explain_init(Explain*);
void explain_reset(Explain*);
Buf* explain(Explain*, Code*, Code*, CodeData*, Plan*, Buf*, bool);

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
