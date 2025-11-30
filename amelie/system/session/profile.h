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

typedef struct Profile Profile;

struct Profile
{
	uint64_t time_run_us;
	uint64_t time_commit_us;
};

static inline void
profile_init(Profile* self)
{
	self->time_run_us    = 0;
	self->time_commit_us = 0;
}

static inline void
profile_reset(Profile* self)
{
	self->time_run_us    = 0;
	self->time_commit_us = 0;
}

static inline void
profile_start(uint64_t* metric)
{
	time_start(metric);
}

static inline void
profile_end(uint64_t* metric)
{
	time_end(metric);
}

static inline void
profile_write(Profile* self, Output* output, Buf* buf)
{
	encode_obj(buf);
	uint64_t time_us = self->time_run_us + self->time_commit_us;

	// time_run
	encode_raw(buf, "time_run_ms", 11);
	encode_real(buf, self->time_run_us / 1000.0);

	// time_commit
	encode_raw(buf, "time_commit_ms", 14);
	encode_real(buf, self->time_commit_us / 1000.0);

	// time
	encode_raw(buf, "time_ms", 7);
	encode_real(buf, time_us / 1000.0);

	// sent_total
	encode_raw(buf, "sent_total", 10);
	encode_integer(buf, buf_size(output->buf));

	encode_obj_end(buf);
}

static inline void
profile_create(Profile* self,
               Program* program,
               Output*  output)
{
	auto buf = buf_create();
	defer_buf(buf);

	encode_obj(buf);

	// explain
	encode_raw(buf, "explain", 7);
	buf_write_buf(buf, &program->explain);

	// profile
	encode_raw(buf, "profile", 7);
	profile_write(self, output, buf);

	encode_obj_end(buf);

	// set new output
	buf_reset(output->buf);

	Str column;
	str_set(&column, "profile", 7);
	output_write_json(output, &column, buf->start, false);
}
