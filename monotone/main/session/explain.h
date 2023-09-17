#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Explain Explain;

struct Explain
{
	bool     active;
	uint64_t time_parse_us;
	uint64_t time_run_us;
	uint64_t time_commit_us;
	uint64_t sent_objects;
	uint64_t sent_size;
	Portal   portal;
};

static inline void
explain_portal(Portal* portal, Buf* buf)
{
	Explain* self = portal->arg;
	self->sent_objects++;
	self->sent_size += buf_size(buf);
	buf_free(buf);
}

static inline void
explain_init(Explain* self)
{
	self->active         = false;
	self->time_parse_us  = 0;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_objects   = 0;
	self->sent_size      = 0;
	portal_init(&self->portal, explain_portal, self);
}

static inline void
explain_free(Explain* self)
{
	unused(self);
}

static inline void
explain_reset(Explain* self)
{
	self->active         = false;
	self->time_parse_us  = 0;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_objects   = 0;
	self->sent_size      = 0;
}

static inline void
explain_start(Explain* self, uint64_t* metric)
{
	if (! self->active)
		return;
	*metric = timer_mgr_gettime();
}

static inline void
explain_end(Explain* self, uint64_t* metric)
{
	if (! self->active)
		return;
	*metric = (timer_mgr_gettime() - *metric) / 1000;
}

#if 0
static inline Buf*
explain(Explain* self, Code* code, bool profile)
{
	in_buf *buf = in_buf_begin(0);

	in_code_dump(code, buf);
	if (profile)
	{
		uint64_t time_us = self->time_parse_us + self->time_run_us +
						   self->time_commit_us;

		in_buf_printf(buf, "%s", "\n");
		in_buf_printf(buf, "%s", "profiler\n");
		in_buf_printf(buf, "%s", "--------\n");
		in_buf_printf(buf, " time parse:   %.3f ms\n", self->time_parse_us / 1000.0);
		in_buf_printf(buf, " time run:     %.3f ms\n", self->time_run_us / 1000.0);
		in_buf_printf(buf, " time commit:  %.3f ms\n", self->time_commit_us / 1000.0);
		in_buf_printf(buf, " time:         %.3f ms\n", time_us / 1000.0);
		in_buf_printf(buf, " sent objects: %d\n",   self->sent_objects);
		in_buf_printf(buf, " sent total:   %d\n",   self->sent_size);
	}

	in_buf *string;
	string = in_string((char*)buf->start, in_buf_size(buf));

	in_buf_abort(buf);
	return string;
}
#endif
