
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_stream.h>

void
stream_init(Stream* self)
{
	self->id         = 0;
	self->subs_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->subs);
	storage_init(&self->storage, STORAGE_STREAM, 256 * 1024);
}

void
stream_free(Stream* self)
{
	storage_free(&self->storage);
	spinlock_free(&self->lock);
}

size_t
stream_create(Stream* self, char* path)
{
	// create stream storage file
	StreamHeader header =
	{
		.id = self->id
	};
	return storage_create(&self->storage, path, (uint8_t*)&header, sizeof(header));
}

size_t
stream_open(Stream* self, char* path)
{
	// read stream storage file
	Buf meta;
	buf_init(&meta);
	defer_buf(&meta);
	auto size_file = storage_open(&self->storage, path, STORAGE_STREAM, &meta);

	// validate stream header size
	if (unlikely(buf_size(&meta) != sizeof(StreamHeader)))
		error("storage: file '{str}' has invalid stream header", path);

	// set id
	auto header = (StreamHeader*)meta.start;
	self->id = header->id;

	return size_file;
}

static void
stream_notify(Stream* self)
{
	// wakeup subscribers
	while (self->subs_count > 0)
	{
		auto sub = container_of(list_pop(&self->subs), StreamSub, link);
		self->subs_count--;
		sub->active = false;
		stream_sub_signal(sub);
	}
}

void
stream_subscribe(Stream* self, StreamSub* sub)
{
	spinlock_lock(&self->lock);
	if (self->id > sub->id)
	{
		stream_sub_signal(sub);
		spinlock_unlock(&self->lock);
		return;
	}
	list_append(&self->subs, &sub->link);
	self->subs_count++;
	sub->active = true;
	spinlock_unlock(&self->lock);
}

void
stream_unsubscribe(Stream* self, StreamSub* sub)
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

void
stream_gc(Stream* self)
{
	spinlock_lock(&self->lock);
	// todo: drop first pages on overfill
	spinlock_unlock(&self->lock);
}

hot static inline void
stream_add(Stream*  self,
           uint64_t id,
           uint8_t* data,
           uint32_t data_size)
{
	// maybe allocate a new page
	auto size = sizeof(StreamEvent) + data_size;
	storage_ensure(&self->storage, size);

	// write event
	auto page  = self->storage.current;
	auto event = (StreamEvent*)page_at(page, page->position);
	event->id        = id;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// advance
	page->position += size;
}

hot void
stream_write(Stream* self, uint8_t* data, uint32_t data_size)
{
	spinlock_lock(&self->lock);

	// assign id and add to the storage
	auto id = ++self->id;
	stream_add(self, id, data, data_size);

	// wakeup subscribers
	stream_notify(self);

	spinlock_unlock(&self->lock);
}

void
stream_state(Stream* self, Buf* buf)
{
	spinlock_lock(&self->lock);

	// {}
	encode_obj(buf);

	// subs
	encode_raw(buf, "subs", 4);
	encode_int(buf, self->subs_count);

	// id
	encode_raw(buf, "id", 2);
	encode_int(buf, self->id);

	// size
	encode_raw(buf, "size", 4);
	encode_int(buf, storage_size(&self->storage));

	// pages
	encode_raw(buf, "pages", 5);
	encode_int(buf, self->storage.list_count);

	encode_obj_end(buf);
	spinlock_unlock(&self->lock);
}

size_t
stream_size(Stream* self)
{
	spinlock_lock(&self->lock);
	auto size = storage_size(&self->storage);
	spinlock_unlock(&self->lock);
	return size;
}
