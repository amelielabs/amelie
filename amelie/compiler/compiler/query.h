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

typedef struct QueryIf      QueryIf;
typedef struct Query        Query;
typedef struct QueryContext QueryContext;

struct QueryIf
{
	Query* (*create)(Local*, SetCache*, Str*);
	void   (*free)(Query*);
	void   (*reset)(Query*);
	void   (*parse)(Query*, QueryContext*);
};

struct Query
{
	QueryIf* iface;
	Str      content_type;
	List     link;
};

struct QueryContext
{
	bool     explain;
	bool     profile;
	Program* program;
	Str*     text;
	Str*     uri;
	Udf*     execute;
	Value*   args;
	Columns* returning;
	Str*     returning_fmt;
};

static inline void
query_context_init(QueryContext* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
query_context_set(QueryContext* self, Program* program, Str* text, Str* uri)
{
	self->explain       = false;
	self->profile       = false;
	self->program       = program;
	self->text          = text;
	self->uri           = uri;
	self->execute       = NULL;
	self->args          = NULL;
	self->returning     = NULL;
	self->returning_fmt = NULL;
}

static inline Query*
query_allocate(QueryIf*  iface,
               Local*    local,
               SetCache* set_cache,
               Str*      content_type)
{
	return iface->create(local, set_cache, content_type);
}

static inline void
query_free(Query* self)
{
	self->iface->free(self);
}

static inline void
query_reset(Query* self)
{
	self->iface->reset(self);
}

static inline void
query_parse(Query* self, QueryContext* context)
{
	self->iface->parse(self, context);
}

extern QueryIf query_sql_if;
extern QueryIf query_csv_if;
extern QueryIf query_json_if;
extern QueryIf query_jsonl_if;
