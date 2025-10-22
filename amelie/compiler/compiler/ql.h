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

typedef struct QlIf      QlIf;
typedef struct Ql        Ql;
typedef struct QlContext QlContext;

struct QlIf
{
	Ql*  (*create)(Local*, Str*);
	void (*free)(Ql*);
	void (*reset)(Ql*);
	void (*parse)(Ql*, QlContext*);
};

struct Ql
{
	QlIf* iface;
	Str   content_type;
	List  link;
};

struct QlContext
{
	bool     explain;
	bool     profile;
	Program* program;
	Str*     text;
	Str*     uri;
};

static inline void
ql_context_init(QlContext* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
ql_context_set(QlContext* self, Program* program, Str* text, Str* uri)
{
	self->explain = false;
	self->profile = false;
	self->program = program;
	self->text    = text;
	self->uri     = uri;
}

static inline Ql*
ql_allocate(QlIf* iface, Local* local, Str* content_type)
{
	return iface->create(local, content_type);
}

static inline void
ql_free(Ql* self)
{
	self->iface->free(self);
}

static inline void
ql_reset(Ql* self)
{
	self->iface->reset(self);
}

static inline void
ql_parse(Ql* self, QlContext* context)
{
	self->iface->parse(self, context);
}

extern QlIf ql_sql_if;
extern QlIf ql_csv_if;
extern QlIf ql_json_if;
extern QlIf ql_jsonl_if;
