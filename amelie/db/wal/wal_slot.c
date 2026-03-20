
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>

void
wal_attach(Wal* self, WalSlot* slot)
{
	assert(! slot->active);
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->active = true;
	spinlock_unlock(&self->lock);
}

void
wal_detach(Wal* self, WalSlot* slot)
{
	if (! slot->active)
		return;
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
	slot->active = false;
}

void
wal_snapshot(Wal* self, WalSlot* slot, Buf* data)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	// create wal slot to ensure listed files exists
	wal_slot_set(slot, 0);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->active = true;

	// [[path, size, mode], ...]
	encode_array(data);
	char path[PATH_MAX];
	for (auto file = self->files; file; file = file->next)
	{
		sfmt(path, sizeof(path), "wal/%" PRIu64, file->id);
		encode_basefile(data, path);
	}
	encode_array_end(data);
}

int
wal_slots(Wal* self, uint64_t* min)
{
	spinlock_lock(&self->lock);
	auto count = self->slots_count;
	list_foreach(&self->slots)
	{
		auto slot = list_at(WalSlot, link);
		auto lsn = atomic_u64_of(&slot->lsn);
		if (*min < lsn)
			*min = lsn;
	}
	spinlock_unlock(&self->lock);
	return count;
}
