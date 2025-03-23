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

typedef struct PlanKey PlanKey;
typedef struct Plan    Plan;

enum
{
	// scan operation with or without key
	// gt, gte, lt, lte
	PLAN_SCAN,

	// point lookup with all keys set
	PLAN_LOOKUP
};

struct PlanKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

struct Plan
{
	int      type;
	Target*  target;
	int      match_start;
	int      match_start_columns;
	int      match_stop;
	PlanKey  keys[];
};

Plan*    plan_create(Target*, Keys*, AstList*);
uint32_t plan_create_hash(Plan*);
