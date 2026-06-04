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

typedef struct Query Query;

typedef enum
{
	QUERY_UNDEF,
	QUERY_SQL,
	QUERY_WRITE
} QueryType;

struct Query
{
	QueryType type;
	Str*      text;
	Str*      rel_user;
	Str*      rel;
	uint8_t*  args;
};

static inline void
query_init(Query* self)
{
	self->type     = QUERY_UNDEF;
	self->args     = NULL;
	self->text     = NULL;
	self->rel_user = NULL;
	self->rel      = NULL;
}

static inline void
query_reset(Query* self)
{
	query_init(self);
}
