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

typedef struct SltCommand SltCommand;

typedef enum
{
	SLT_UNDEF,
	SLT_QUERY,
	SLT_STATEMENT_OK,
	SLT_STATEMENT_ERROR,
	SLT_HASH_THRESHOLD,
	SLT_HALT
} SltCommandType;

struct SltCommand
{
	SltCommandType type;
	Str            command;
	Str            query;
	Str            result;
	Str            body;
	int            line;
};

static inline void
slt_command_init(SltCommand* self)
{
	self->type = SLT_UNDEF;
	self->line = 0;
	str_init(&self->command);
	str_init(&self->query);
	str_init(&self->result);
	str_init(&self->body);
}

static inline void
slt_command_reset(SltCommand* self)
{
	slt_command_init(self);
}

static inline void
slt_command_match(SltCommand* self)
{
	if (str_is_prefix(&self->command, "query", 5))
	{
		self->type = SLT_QUERY;
	} else
	if (str_is(&self->command, "statement ok", 12))
	{
		self->type = SLT_STATEMENT_OK;
	} else
	if (str_is(&self->command, "statement error", 16))
	{
		self->type = SLT_STATEMENT_ERROR;
	} else
	if (str_is_prefix(&self->command, "hash-threshold", 14))
	{
		self->type = SLT_HASH_THRESHOLD;
	} else
	if (str_is(&self->command, "halt", 4))
	{
		self->type = SLT_HALT;
	} else
	{
		self->type = SLT_UNDEF;
	}
}
