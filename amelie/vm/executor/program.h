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
	Buf      explain;
	SetList  sets;
	int      send_last;
	bool     snapshot;
	bool     repl;
	bool     utility;
	LockId   lock_catalog;
	LockId   lock_ddl;
};

static inline Program*
program_allocate(void)
{
	auto self = (Program*)am_malloc(sizeof(Program));
	self->send_last    = -1;
	self->snapshot     = false;
	self->repl         = false;
	self->utility      = false;
	self->lock_catalog = LOCK_SHARED;
	self->lock_ddl     = LOCK_NONE;
	code_init(&self->code);
	code_init(&self->code_backend);
	code_data_init(&self->code_data);
	access_init(&self->access);
	buf_init(&self->explain);
	set_list_init(&self->sets);
	return self;
}

static inline void
program_free(Program* self)
{
	if (self->sets.list_count)
		set_list_free(&self->sets);
	code_free(&self->code);
	code_free(&self->code_backend);
	code_data_free(&self->code_data);
	access_free(&self->access);
	buf_free(&self->explain);
	am_free(self);
}

static inline void
program_reset(Program* self, SetCache* cache)
{
	self->send_last    = -1;
	self->snapshot     = false;
	self->repl         = false;
	self->utility      = false;
	self->lock_catalog = LOCK_SHARED;
	self->lock_ddl     = LOCK_NONE;
	code_reset(&self->code);
	code_reset(&self->code_backend);
	code_data_reset(&self->code_data);
	access_reset(&self->access);
	buf_reset(&self->explain);
	set_cache_push_list(cache, &self->sets);
}

static inline bool
program_empty(Program* self)
{
	return !code_count(&self->code);
}
