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

typedef struct Ring Ring;

struct Ring
{
	atomic_uint_fast64_t pos_claim;
	atomic_uint_fast64_t pos_read;
	atomic_uint_fast64_t pos_write;
	void**               ring;
	int                  ring_size;
};

static inline void
ring_init(Ring* self)
{
	self->pos_claim = 0;
	self->pos_read  = 0;
	self->pos_write = 0;
	self->ring      = NULL;
	self->ring_size = 0;
}

static inline void
ring_free(Ring* self)
{
	if (self->ring)
	{
		am_free(self->ring);
		self->ring = NULL;
		self->ring_size = 0;
	}
}

static inline void
ring_prepare(Ring* self, int size)
{
	self->ring_size = size;
	self->ring = am_malloc(sizeof(void*) * size);
	memset(self->ring, 0, sizeof(void*) * size);
}

hot static inline void
ring_write(Ring* self, void* data)
{
	// claim a slot in the ring buffer
	uint64_t slot = atomic_fetch_add_explicit(&self->pos_claim, 1, memory_order_relaxed);

	// wait until the slot is available for writing
	//
	// This prevents overwriting data that the consumer has not yet read.
	// This is where the channel blocks if full.
	//
	while (slot >= atomic_load_explicit(&self->pos_read, memory_order_acquire) + self->ring_size) {
		__asm__("pause");
	}

	// write the data to the claimed slot.
	self->ring[slot & (self->ring_size - 1)] = data;

	// wait for our turn to publish. This ensures that writes are published
	// in the same order they were claimed, maintaining sequence
	while (atomic_load_explicit(&self->pos_write, memory_order_relaxed) != slot) {
		__asm__("pause");
	}

	// publish the write, making it visible to the consumer.
	//
	// The release semantics ensures that the write to the buffer is visible
	// before the cursor is updated.
	atomic_store_explicit(&self->pos_write, slot + 1, memory_order_release);
}

hot static inline void*
ring_read(Ring* self)
{
	// The acquire semantics on pos_write ensures we see the latest writes
	// from the producer before we attempt to read.
	uint64_t pos_read  = atomic_load_explicit(&self->pos_read, memory_order_relaxed);
	uint64_t pos_write = atomic_load_explicit(&self->pos_write, memory_order_acquire);

	// ring is empty
	if (pos_read == pos_write)
		return NULL;

	// read the data
	auto data = self->ring[pos_read & (self->ring_size - 1)];

	// make the slot available for producers.
	//
	// The release semantics ensures that producers see the updated read cursor.
	atomic_store_explicit(&self->pos_read, pos_read + 1, memory_order_release);
	return data;
}
