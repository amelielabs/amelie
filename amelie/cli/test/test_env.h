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

typedef struct TestEnv TestEnv;

struct TestEnv
{
	Str  name;
	Cli  cli;
	int  sessions;
	List link;
};

static inline TestEnv*
test_env_create(Str* name)
{
	TestEnv* self = am_malloc(sizeof(*self));
	str_copy(&self->name, name);
	self->sessions = 0;
	cli_init(&self->cli);
	list_init(&self->link);
	return self;
}

static inline void
test_env_free(TestEnv* self)
{
	cli_stop(&self->cli);
	cli_free(&self->cli);
	str_free(&self->name);
	am_free(self);
}
