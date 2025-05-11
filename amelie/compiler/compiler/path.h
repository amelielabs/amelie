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

typedef struct PathKey PathKey;
typedef struct Path    Path;

enum
{
	// scan operation with or without key
	// gt, gte, lt, lte
	PATH_SCAN,

	// point lookup with all keys set
	PATH_LOOKUP
};

struct PathKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

struct Path
{
	int      type;
	Target*  target;
	int      match_start;
	int      match_start_columns;
	int      match_start_vars;
	int      match_stop;
	PathKey  keys[];
};

Path*    path_create(Target*, Scope*, Keys*, AstList*);
uint32_t path_create_hash(Path*);
