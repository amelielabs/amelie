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

typedef struct Dep  Dep;
typedef struct Deps Deps;
typedef struct Stmt Stmt;

typedef enum
{
	DEP_STMT,
	DEP_VAR
} DepType;

struct Dep
{
	DepType type;
	Stmt*   stmt;
	Dep*    next;
};

struct Deps
{
	Dep* list;
	Dep* list_tail;
	int  count;
};

static inline void
deps_init(Deps* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
deps_reset(Deps* self)
{
	deps_init(self);
}

static inline Dep*
deps_find(Deps* self, Stmt* stmt)
{
	auto dep = self->list;
	for (; dep; dep = dep->next) {
		if (dep->stmt == stmt)
			return dep;
	}
	return NULL;
}

static inline void
deps_add(Deps* self, DepType type, Stmt* stmt)
{
	if (deps_find(self, stmt))
		return;
	auto dep = (Dep*)palloc(sizeof(Dep));
	dep->type = type;
	dep->stmt = stmt;
	dep->next = NULL;

	if (self->list == NULL)
		self->list = dep;
	else
		self->list_tail->next = dep;
	self->list_tail = dep;
	self->count++;
}
