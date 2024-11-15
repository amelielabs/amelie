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

typedef struct RowMeta RowMeta;
typedef struct RowData RowData;

struct RowMeta
{
	uint32_t offset;
	uint32_t offset_index;
	uint32_t columns;
	uint32_t hash;
};

struct RowData
{
	Buf      data;
	Buf      data_index;
	Buf      data_meta;
	RowMeta* current;
	int      rows;
};

static inline void
row_data_init(RowData* self)
{
	self->current = NULL;
	self->rows    = 0;
	buf_init(&self->data);
	buf_init(&self->data_index);
	buf_init(&self->data_meta);
}

static inline void
row_data_free(RowData* self)
{
	buf_free(&self->data);
	buf_free(&self->data_index);
	buf_free(&self->data_meta);
}

static inline void
row_data_reset(RowData* self)
{
	self->current = NULL;
	self->rows    = 0;
	buf_reset(&self->data);
	buf_reset(&self->data_index);
	buf_reset(&self->data_meta);
}

hot static inline RowMeta*
row_data_at(RowData* self, int pos)
{
	return &((RowMeta*)self->data_meta.start)[pos];
}

hot static inline void
row_data_begin(RowData* self, int columns)
{
	auto meta = (RowMeta*)buf_claim(&self->data_meta, sizeof(RowMeta));
	meta->offset       = buf_size(&self->data);
	meta->offset_index = buf_size(&self->data_index);
	meta->columns      = columns;
	meta->hash         = 0;
	self->current      = meta;
	self->rows++;
}

hot static inline Buf*
row_data_add(RowData* self)
{
	uint32_t offset = buf_size(&self->data);
	buf_write(&self->data_index, &offset, sizeof(offset));
	return &self->data;
}

hot static inline void
row_data_set_null(RowData* self)
{
	// set last column offset to 0
	*(uint32_t*)(self->data_index.position - sizeof(uint32_t)) = 0;
}

hot static inline void
row_data_hash(RowData* self)
{
	// hash last column data
	auto offset = *(uint32_t*)(self->data_index.position - sizeof(uint32_t));
	auto size = buf_size(&self->data) - offset;
	assert(size > 0);
	self->current->hash =
		hash_murmur3_32(self->data.start + offset, size,
		                self->current->hash);
}

hot static inline Row*
row_data_create(RowData* self, int pos)
{
	int  size;
	auto meta = row_data_at(self, pos);
	auto meta_next = meta + 1;
	if ((uint8_t*)meta_next >= self->data_meta.position)
		size = buf_size(&self->data) - meta->offset;
	else
		size = meta_next->offset - meta->offset;
	auto col = buf_u32(&self->data_index) + meta->offset_index;

	auto row = row_allocate(meta->columns, size);
	for (uint32_t i = 0; i < meta->columns; i++)
	{
		if (row->size_factor == 0)
		{
			if (col[i])
				row->u8[1 + i] = sizeof(Row) + sizeof(uint8_t) * (1 + meta->columns) +
				                 (col[i] - meta->offset);
			else
				row->u8[1 + i] = 0;
		} else
		if (row->size_factor == 1)
		{
			if (col[i])
				row->u16[1 + i] = sizeof(Row) + sizeof(uint16_t) * (1 + meta->columns) +
				                  (col[i] - meta->offset);
			else
				row->u16[1 + i] = 0;
		} else
		{
			if (col[i])
				row->u32[1 + i] = sizeof(Row) + sizeof(uint32_t) * (1 + meta->columns) +
				                  (col[i] - meta->offset);
			else
				row->u32[1 + i] = 0;
		}
	}
	memcpy(row_data(row, meta->columns), self->data.start + meta->offset, size);
	return row;
}
