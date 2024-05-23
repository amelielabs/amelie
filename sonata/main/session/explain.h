#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Explain Explain;

struct Explain
{
	bool     active;
	uint64_t time_run_us;
	uint64_t time_commit_us;
	uint64_t sent_size;
};

void explain_init(Explain*);
void explain_reset(Explain*);
Buf* explain(Explain*, Code*, Code*, CodeData*, Plan*, bool);

static inline void
explain_start(Explain* self, uint64_t* metric)
{
	if (! self->active)
		return;
	time_start(metric);
}

static inline void
explain_end(Explain* self, uint64_t *metric)
{
	if (! self->active)
		return;
	time_end(metric);
}
