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

typedef struct Job Job;

typedef void (*JobFunction)(int, intptr_t*);

struct Job
{
	JobFunction main;
	int         argc;
	intptr_t*   argv;
	Error*      error;
	Event       on_complete;
	List        link;
};

static inline void
job_init(Job* self, Error* error, JobFunction main, int argc, intptr_t* argv)
{
	self->main  = main;
	self->argc  = argc;
	self->argv  = argv;
	self->error = error;
	event_init(&self->on_complete);
	list_init(&self->link);
}

static inline void
job_run(Job* self)
{
	if (error_catch( self->main(self->argc, self->argv) ))
		error_copy(self->error, &am_self()->error);
	event_signal(&self->on_complete);
}
