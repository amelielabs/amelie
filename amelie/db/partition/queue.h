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

typedef struct QueueNode QueueNode;
typedef struct Queue     Queue;

struct QueueNode
{
	uint64_t id;
	int      id_step;
	bool     last;
	List     link;
};

struct Queue
{
	bool     lock;
	uint64_t lock_id;
	List     list;
	int      list_count;
};

static inline void
queue_node_init(QueueNode* self)
{
	self->id      = 0;
	self->id_step = 0;
	self->last    = false;
	list_init(&self->link);
}

static inline void
queue_node_reset(QueueNode* self)
{
	self->id      = 0;
	self->id_step = 0;
	self->last    = false;
}

static inline void
queue_init(Queue* self)
{
	self->lock       = false;
	self->lock_id    = 0;
	self->list_count = 0;
	list_init(&self->list);
}

hot static inline void
queue_add(Queue* self, QueueNode* node)
{
	// nodes are ordered by [id, id_step]
	list_foreach_reverse_safe(&self->list)
	{
		auto ref = list_at(QueueNode, link);
		if (ref->id > node->id)
			continue;

		if (ref->id == node->id) {
			assert(ref->id_step < node->id_step);
		}

		list_insert_after(&ref->link, &node->link);
		self->list_count++;
		return;
	}

	// head
	list_push(&self->list, &node->link);
	self->list_count++;
}

hot static inline QueueNode*
queue_begin(Queue* self)
{
	if (! self->list_count)
		return NULL;

	// queue is locked (first node is active)
	if (self->lock)
		return NULL;

	auto node = container_of(list_first(&self->list), QueueNode, link);
	if (self->lock_id > 0)
	{
		// previously completed node was not last, next node must
		// have the same lock id
		if (node->id != self->lock_id)
			return NULL;
	} else {
		self->lock_id = node->id;
	}

	self->lock = true;
	return node;
}

hot static inline void
queue_end(Queue* self, QueueNode* node)
{
	// completed node is always first
	assert(self->list_count);
	assert(container_of(list_first(&self->list), QueueNode, link) == node);
	assert(self->lock);
	assert(self->lock_id == node->id);

	// unlock queue and reset lock id, if the node was last
	self->lock = false;
	if (node->last)
		self->lock_id = 0;

	list_pop(&self->list);
	self->list_count--;
}
