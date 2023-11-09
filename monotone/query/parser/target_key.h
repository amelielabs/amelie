#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TargetKey TargetKey;

struct TargetKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

static inline TargetKey*
target_key_create(Def* def)
{
	TargetKey* keys;
	keys = palloc(sizeof(TargetKey) * def->key_count);
	auto key = def->key;
	for (; key; key = key->next)
	{
		auto ref = &keys[key->order];
		ref->key      = key;
		ref->start_op = NULL;
		ref->start    = NULL;
		ref->stop_op  = NULL;
		ref->stop     = NULL;
	}
	return keys;
}
