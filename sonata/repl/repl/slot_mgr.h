#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct SlotMgr SlotMgr;

struct SlotMgr
{
	List list;
	int  list_count;
	Wal* wal;
};

static inline void
slot_mgr_init(SlotMgr* self, Wal* wal)
{
	self->wal = wal;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
slot_mgr_free(SlotMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto slot = list_at(Slot, link);
		wal_del(self->wal, &slot->slot);
		slot_free(slot);
	}
}

static inline void
slot_mgr_create(SlotMgr* self, Node* node)
{
	auto slot = slot_allocate(node, self->wal);
	wal_slot_set(&slot->slot, 0);
	wal_add(self->wal, &slot->slot);
	list_append(&self->list, &slot->link);
	self->list_count++;
}

static inline void
slot_mgr_drop(SlotMgr* self, Slot* slot)
{
	// slot pusher must be stopped
	wal_del(self->wal, &slot->slot);
	list_unlink(&slot->link);
	self->list_count--;
	slot_free(slot);
}

static inline Slot*
slot_mgr_find(SlotMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto slot = list_at(Slot, link);
		if (uuid_compare(&slot->node->config->id, id))
			return slot;
	}
	return NULL;
}

static inline void
slot_mgr_open(SlotMgr* self, NodeMgr* node_mgr)
{
	list_foreach_safe(&node_mgr->list)
	{
		auto node = list_at(Node, link);
		slot_mgr_create(self, node);
	}
}
