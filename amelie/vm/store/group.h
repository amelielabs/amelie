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

typedef struct GroupAgg  GroupAgg;
typedef struct GroupNode GroupNode;
typedef struct Group     Group;

enum
{
	GROUP_COUNT,
	GROUP_MIN,
	GROUP_MAX,
	GROUP_SUM,
	GROUP_AVG,
	GROUP_LAMBDA
};

struct GroupAgg
{
	int   type;
	int   type_agg;
	Value value;
};

struct GroupNode
{
	HashtableNode node;
	Value         value[];
};

struct Group
{
	Store     store;
	Hashtable ht;
	Buf       aggs;
	int       aggs_count;
	int       keys_count;
};

Group*
group_create(int);
void group_add(Group*, int, Value*);
void group_write(Group*, Stack*);
void group_get(Group*, Stack*, int, Value*);
void group_read_aggr(Group*, GroupNode*, int, Value*);
void group_read_keys(Group*, GroupNode*, Value*);

static inline GroupAgg*
group_agg(Group* self, int pos)
{
	auto aggs = (GroupAgg*)self->aggs.start;
	return &aggs[pos];
}

static inline GroupNode*
group_at(Group* self, int pos)
{
	auto node = hashtable_at(&self->ht, pos);
	return container_of(node, GroupNode, node);
}

static inline int
group_next(Group* self, int pos)
{
	return hashtable_next(&self->ht, pos);
}
