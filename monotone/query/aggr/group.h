#pragma once

//
// monotone
//
// SQL OLTP database
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
	ValueObj  obj;
	Hashtable ht;
	List      aggrs;
	int       aggr_count;
	int       aggr_size;
	int       keys_count;
};

Group*
group_create(int);
void group_add_aggr(Group*, AggrIf*);
void group_add(Group*, Stack*);
void group_get_aggr(Group*, GroupNode*, int, Value*);
void group_get(Group*, GroupNode*, Value*);

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
