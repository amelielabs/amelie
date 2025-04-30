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

typedef struct Program Program;

struct Program
{
	Code     code;
	Code     code_backend;
	CodeData code_data;
	Access   access;
	int      sends;
	bool     snapshot;
	bool     repl;
};

static inline Program*
program_allocate(void)
{
	auto self = (Program*)am_malloc(sizeof(Program));
	self->sends    = 0;
	self->snapshot = false;
	self->repl     = false;
	code_init(&self->code);
	code_init(&self->code_backend);
	code_data_init(&self->code_data);
	access_init(&self->access);
	return self;
}

static inline void
program_free(Program* self)
{
	code_free(&self->code);
	code_free(&self->code_backend);
	code_data_free(&self->code_data);
	access_free(&self->access);
	am_free(self);
}

static inline void
program_reset(Program* self)
{
	self->sends    = 0;
	self->snapshot = false;
	self->repl     = false;
	code_reset(&self->code);
	code_reset(&self->code_backend);
	code_data_reset(&self->code_data);
	access_reset(&self->access);
}
