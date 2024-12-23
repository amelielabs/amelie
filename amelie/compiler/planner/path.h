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

typedef struct AstPathKey AstPathKey;
typedef struct AstPath    AstPath;
typedef struct Path       Path;

enum
{
	// scan operation with or without key
	// gt, gte, lt, lte
	PATH_SCAN,

	// point lookup with all keys set
	PATH_LOOKUP
};

struct AstPathKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

struct AstPath
{
	Ast         ast;
	int         type;
	int         match;
	int         match_stop;
	AstPathKey* keys;
	AstPath*    list;
	AstPath*    next;
};

struct Path
{
	Keys*   keys;
	Target* target;
};

static inline AstPath*
ast_path_of(Ast* ast)
{
	return (AstPath*)ast;
}

static inline void
path_init(Path* self, Target* target, Keys* keys)
{
	self->keys   = keys;
	self->target = target;
}

AstPath* path_create(Path*, Ast*);
