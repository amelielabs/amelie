
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
#include <amelie_cdc.h>

void
cdc_init(Cdc* self)
{
	self->lsn         = 0;
	self->subs_count  = 0;
	self->slots_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->subs);
	list_init(&self->slots);
	storage_init(&self->storage, STORAGE_CDC, 256 * 1024);
}

void
cdc_free(Cdc* self)
{
	storage_free(&self->storage);
	spinlock_free(&self->lock);
}

size_t
cdc_create(Cdc* self, char* path)
{
	// create cdc storage file
	CdcHeader header =
	{
		.lsn = self->lsn
	};
	return storage_create(&self->storage, path, (uint8_t*)&header, sizeof(header));
}

size_t
cdc_open(Cdc* self, char* path)
{
	// read cdc storage file
	Buf meta;
	buf_init(&meta);
	defer_buf(&meta);
	auto size_file = storage_open(&self->storage, path, STORAGE_CDC, &meta);

	// validate cdc header size
	if (unlikely(buf_size(&meta) != sizeof(CdcHeader)))
		error("storage: file '{str}' has invalid cdc header", path);

	// set lsn
	auto header = (CdcHeader*)meta.start;
	self->lsn = header->lsn;

	return size_file;
}

static void
cdc_notify(Cdc* self)
{
	// wakeup subscribers
	while (self->subs_count > 0)
	{
		auto sub = container_of(list_pop(&self->subs), CdcSub, link);
		self->subs_count--;
		sub->active = false;
		cdc_sub_signal(sub);
	}
}

void
cdc_attach(Cdc* self, CdcSlot* slot)
{
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	spinlock_unlock(&self->lock);
	slot->attached = true;
}

void
cdc_detach(Cdc* self, CdcSlot* slot)
{
	if (! slot->attached)
		return;
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
	slot->attached = false;
}

void
cdc_subscribe(Cdc* self, CdcSub* sub)
{
	spinlock_lock(&self->lock);
	if (self->lsn > sub->lsn)
	{
		cdc_sub_signal(sub);
		spinlock_unlock(&self->lock);
		return;
	}
	list_append(&self->subs, &sub->link);
	self->subs_count++;
	sub->active = true;
	spinlock_unlock(&self->lock);
}

void
cdc_unsubscribe(Cdc* self, CdcSub* sub)
{
	spinlock_lock(&self->lock);
	if (sub->active)
	{
		list_unlink(&sub->link);
		self->subs_count--;
		sub->active = false;
	}
	spinlock_unlock(&self->lock);
}

static inline uint64_t
cdc_minof(Cdc* self)
{
	// get min across all slots
	uint64_t min = UINT64_MAX;
	list_foreach_safe(&self->slots)
	{
		auto slot = list_at(CdcSlot, link);
		auto lsn  = atomic_u64_of(&slot->lsn);
		if (lsn < min)
			min = lsn;
	}
	return min;
}

void
cdc_min(Cdc* self, uint64_t* minref)
{
	spinlock_lock(&self->lock);
	*minref = cdc_minof(self);
	spinlock_unlock(&self->lock);
}

void
cdc_gc(Cdc* self)
{
	spinlock_lock(&self->lock);

	// get min across all slots
	uint64_t min = cdc_minof(self);

	// remove all pages < min (keep at least one)
	while (self->storage.list_count > 1)
	{
		// first event on the second page
		auto second = storage_at(&self->storage, 1);
		auto ev = (CdcEvent*)second->data;
		if (ev->lsn < min)
		{
			// free the first page
			auto first = storage_pop(&self->storage);
			spinlock_unlock(&self->lock);

			page_free(first);

			spinlock_lock(&self->lock);
			continue;
		}
		break;
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
cdc_add(Cdc*     self,
        uint64_t lsn,
        uint32_t lsn_op,
        int      cmd,
        Uuid*    id,
        uint8_t* data,
        uint32_t data_size)
{
	// maybe allocate a new page
	auto size = sizeof(CdcEvent) + data_size;
	storage_ensure(&self->storage, size);

	// write event
	auto page  = self->storage.current;
	auto event = (CdcEvent*)page_at(page, page->position);
	event->lsn       = lsn;
	event->lsn_op    = lsn_op;
	event->cmd       = cmd;
	event->id        = *id;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// advance
	page->position += size;

	// set last lsn
	self->lsn = lsn;
}

hot void
cdc_write(Cdc* self, uint64_t lsn, LogCdc* write)
{
	spinlock_lock(&self->lock);

	// add event to the queue
	uint32_t op = 0;
	auto pos = (LogCdcRecord*)write->data.start;
	auto end = (LogCdcRecord*)write->data.position;
	while (pos < end)
	{
		cdc_add(self, lsn, op, pos->cmd, pos->id, pos->data, pos->data_size);
		op++;
		pos = (LogCdcRecord*)(pos->data + pos->data_size);
	}

	// wakeup subscribers
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}

hot void
cdc_write_batch(Cdc* self, uint64_t lsn, List* batch)
{
	spinlock_lock(&self->lock);

	// add all records from the batch
	uint32_t op = 0;
	list_foreach(batch)
	{
		auto ref = list_at(LogCdc, link);
		auto pos = (LogCdcRecord*)ref->data.start;
		auto end = (LogCdcRecord*)ref->data.position;
		while (pos < end)
		{
			cdc_add(self, lsn, op, pos->cmd, pos->id, pos->data, pos->data_size);
			op++;
			pos = (LogCdcRecord*)(pos->data + pos->data_size);
		}
	}

	// wakeup subscribers
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}
