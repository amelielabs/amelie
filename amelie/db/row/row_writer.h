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

typedef struct RowWriter RowWriter;
typedef struct RowMeta   RowMeta;

struct RowMeta
{
	uint32_t offset;
	uint32_t offset_index;
	uint32_t columns;
	uint32_t hash;
};

struct RowWriter
{
	Buf      data;
	Buf      data_index;
	Buf      data_meta;
	RowMeta* current;
};

static inline void
row_writer_init(RowWriter* self)
{
	self->current = NULL;
	buf_init(&self->data);
	buf_init(&self->data_index);
	buf_init(&self->data_meta);
}

static inline void
row_writer_free(RowWriter* self)
{
	buf_free(&self->data);
	buf_free(&self->data_index);
	buf_free(&self->data_meta);
}

static inline void
row_writer_reset(RowWriter* self)
{
	self->current = NULL;
	buf_reset(&self->data);
	buf_reset(&self->data_index);
	buf_reset(&self->data_meta);
}

hot static inline RowMeta*
row_writer_at(RowWriter* self, int pos)
{
	return &((RowMeta*)self->data_meta.start)[pos];
}

hot static inline void
row_writer_begin(RowWriter* self, int columns)
{
	auto meta = (RowMeta*)buf_claim(&self->data_meta, sizeof(RowMeta));
	meta->offset       = buf_size(&self->data);
	meta->offset_index = buf_size(&self->data_index);
	meta->columns      = columns;
	meta->hash         = 0;
	self->current      = meta;
}

hot static inline Buf*
row_writer_add(RowWriter* self)
{
	uint32_t offset = buf_size(&self->data);
	buf_write(&self->data_index, &offset, sizeof(offset));
	return &self->data;
}

hot static inline void
row_writer_add_hash(RowWriter* self, void* data, int data_size)
{
	self->current->hash =
		hash_murmur3_32(data, data_size, self->current->hash);
}

hot static inline Row*
row_writer_create(RowWriter* self, int pos)
{
	int  size;
	auto meta = row_writer_at(self, pos);
	auto meta_next = meta + 1;
	if ((uint8_t*)meta_next >= self->data_meta.position)
		size = buf_size(&self->data) - meta->offset;
	else
		size = meta_next->offset - meta->offset;
	auto col = buf_u32(&self->data_index) + meta->offset_index;
	auto row = row_allocate(meta->columns, size);
	for (uint32_t i = 0; i < meta->columns; i++)
		row_set(row, i, col[i]);
	return row;
}
