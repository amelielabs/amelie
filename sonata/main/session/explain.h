#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Explain Explain;

struct Explain
{
	bool     active;
	uint64_t time_run_us;
	uint64_t time_commit_us;
	uint64_t sent_objects;
	uint64_t sent_size;
	Portal   portal;
};

void explain_init(Explain*);
void explain_reset(Explain*);
Buf* explain(Explain*, Code*, CodeData*, Dispatch*, bool);

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
