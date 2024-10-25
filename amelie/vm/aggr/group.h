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

typedef struct GroupNode GroupNode;
typedef struct Group     Group;

struct GroupNode
{
	HashtableNode node;
	uint32_t      aggr_offset;
	Value         keys[];
	// aggrs
};

struct Group
{
	Store     store;
	Hashtable ht;
	List      aggrs;
	int       aggr_count;
	int       aggr_size;
	int       keys_count;
};

Group*
group_create(int);
void group_add(Group*, AggrIf*, Value*);
void group_write(Group*, Stack*);
void group_get(Group*, Stack*, int, Value*);
void group_read_aggr(Group*, GroupNode*, int, Value*);
void group_read(Group*, GroupNode*, Value*);

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
