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

typedef struct QlIf QlIf;
typedef struct Ql   Ql;

struct QlIf
{
	Ql*  (*create)(Local*, Str*);
	void (*free)(Ql*);
	void (*reset)(Ql*);
	void (*parse)(Ql*, Program*, Str*, Str*);
};

struct Ql
{
	QlIf* iface;
	Str   application_type;
	List  link;
};

static inline Ql*
ql_allocate(QlIf* iface, Local* local, Str* application_type)
{
	return iface->create(local, application_type);
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
ql_parse(Ql* self, Program* program, Str* text, Str* uri)
{
	self->iface->parse(self, program, text, uri);
}

extern QlIf ql_sql_if;
extern QlIf ql_csv_if;
extern QlIf ql_json_if;
extern QlIf ql_jsonl_if;
