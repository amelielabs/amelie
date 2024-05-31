#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Slot Slot;

struct Slot
{
	Pusher  pusher;
	WalSlot slot;
	Node*   node;
	List    link;
};

static inline Slot*
slot_allocate(Node* node, Wal* wal)
{
	auto self = (Slot*)so_malloc(sizeof(Slot));
	self->node = node;
	pusher_init(&self->pusher, wal, &self->slot, node);
	wal_slot_init(&self->slot);
	list_init(&self->link);
	return self;
}

static inline void
slot_free(Slot* self)
{
	pusher_free(&self->pusher);
	so_free(self);
}
