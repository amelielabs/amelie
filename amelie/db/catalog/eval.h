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

typedef struct EvalIf EvalIf;
typedef struct Eval   Eval;

struct EvalIf
{
	void (*create)(Eval*);
	void (*free)(Eval*);
	void (*execute)(Eval*, Str*);
};

struct Eval
{
	EvalIf* iface;
	void*   iface_arg;
	void*   state;
};

static inline void
eval_init(Eval* self, EvalIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->state     = NULL;
}

static inline void
eval_create(Eval* self)
{
	self->iface->create(self);
}

static inline void
eval_free(Eval* self)
{
	if (self->state)
		self->iface->free(self);
	self->state = NULL;
}

static inline void
eval_execute(Eval* self, Str* command)
{
	self->iface->execute(self, command);
}
