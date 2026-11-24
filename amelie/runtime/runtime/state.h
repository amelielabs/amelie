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

typedef struct State State;

enum
{
	RECOVER_NONE,
	RECOVER_CHECKPOINT,
	RECOVER_WAL,
	RECOVER_REPL
};

struct State
{
	Str        directory;
	atomic_u64 lsn;
	atomic_u32 recover;
	Str        secret;
	int        secret_seq;
	Uuid       primary;

};

static inline void
state_init(State* self)
{
	self->lsn        = 0;
	self->recover    = RECOVER_NONE;
	self->secret_seq = 0;
	str_init(&self->directory);
	str_init(&self->secret);
	uuid_init(&self->primary);
}

static inline void
state_free(State* self)
{
	str_free(&self->directory);
	str_free(&self->secret);
}

static inline  void
state_prepare(State* self)
{
	unused(self);
}
