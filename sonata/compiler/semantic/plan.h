#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct AstPlanKey AstPlanKey;
typedef struct AstPlan    AstPlan;

enum
{
	// scan operation with or without key
	// gt, gte, lt, lte
	SCAN,

	// point lookup with all keys set
	SCAN_LOOKUP
};

struct AstPlanKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

struct AstPlan
{
	Ast         ast;
	int         type;
	AstPlanKey* keys;
	AstPlan*    list;
	AstPlan*    next;
};

static inline AstPlan*
ast_plan_of(Ast* ast)
{
	return (AstPlan*)ast;
}

static inline AstPlan*
ast_plan_allocate(Def* def, int type)
{
	AstPlan* self;
	self = ast_allocate(0, sizeof(AstPlan));
	self->type = type;
	self->keys = (AstPlanKey*)palloc(sizeof(AstPlanKey) * def->key_count);
	auto key = def->key;
	for (; key; key = key->next)
	{
		auto ref = &self->keys[key->order];
		ref->key      = key;
		ref->start_op = NULL;
		ref->start    = NULL;
		ref->stop_op  = NULL;
		ref->stop     = NULL;
	}
	self->list = NULL;
	self->next = NULL;
	return self;
}

AstPlan* plan(TargetList*, Target*, Ast*);
