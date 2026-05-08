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
profile_create(Profile* self,
               Program* program,
               Output*  output)
{
	auto buf = buf_create();
	defer_buf(buf);

	// explain
	buf_write_buf(buf, &program->explain);

	buf_format(buf, "\nprofile\n");
	uint64_t time_us = self->time_run_us + self->time_commit_us;
	buf_format(buf, "  {.2f} time_run_ms\n", self->time_run_us / 1000.0);
	buf_format(buf, "  {.2f} time_commit_ms\n", self->time_commit_us / 1000.0);
	buf_format(buf, "  {.2f} time_ms\n", time_us / 1000.0);
	buf_format(buf, "  {.2f} sent_total\n", buf_size(output->buf));

	// set new output
	buf_reset(output->buf);
	Str column;
	str_set(&column, "profile", 7);
	Str str;
	buf_str(buf, &str);
	output_str(output, &column, &str);
}
