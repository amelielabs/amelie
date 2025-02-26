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

typedef struct Row Row;

struct Row
{
	uint8_t size_factor: 2;
	uint8_t unused: 6;
	uint8_t data[];
} packed;

hot static inline Row*
row_allocate(int columns, int data_size)
{
	// [row_header][size + index][data]

	// calculate total size
	int      size_factor = 0;
	uint32_t total_base = sizeof(Row) + data_size;
	uint32_t total = total_base + sizeof(uint8_t) * (1 + columns);
	if (total > UINT8_MAX)
	{
		total = total_base + sizeof(uint16_t) * (1 + columns);
		if (total > UINT16_MAX)
		{
			total = total_base + sizeof(uint32_t) * (1 + columns);
			size_factor = 3;
		} else {
			size_factor = 1;
		}
	}

	// allocate row
	Row* self = am_malloc(total);
	self->size_factor = size_factor;
	self->unused      = 0;

	// set size
	if (self->size_factor == 0)
		*self->data = total;
	else
	if (self->size_factor == 1)
		*(uint16_t*)self->data = total;
	else
		*(uint32_t*)self->data = total;
	return self;
}

always_inline static inline void
row_free(Row* self)
{
	am_free(self);
}

always_inline hot static inline uint32_t
row_size(Row* self)
{
	if (self->size_factor == 0)
		return *self->data;
	if (self->size_factor == 1)
		return *(uint16_t*)self->data;
	return *(uint32_t*)self->data;
}

always_inline hot static inline void*
row_at(Row* self, int column)
{
	register uint32_t offset;
	if (self->size_factor == 0)
		offset = self->data[1 + column];
	else
	if (self->size_factor == 1)
		offset = ((uint16_t*)self->data)[1 + column];
	else
		offset = ((uint32_t*)self->data)[1 + column];
	if (offset == 0)
		return NULL;
	return (uint8_t*)self + offset;
}

always_inline hot static inline void*
row_data(Row* self, int columns)
{
	return self->data + ((1 + columns) * (self->size_factor + 1));
}

always_inline hot static inline uint32_t
row_data_size(Row* self, int columns)
{
	return row_size(self) - sizeof(Row) +
	       ((1 + columns) * (self->size_factor + 1));
}

always_inline hot static inline void
row_set(Row* self, int column, int offset)
{
	if (self->size_factor == 0)
		self->data[1 + column]  = offset;
	else
	if (self->size_factor == 1)
		((uint16_t*)self->data)[1 + column] = offset;
	else
		((uint32_t*)self->data)[1 + column] = offset;
}

always_inline hot static inline void
row_set_null(Row* self, int column)
{
	row_set(self, column, 0);
}

hot static inline uint32_t
row_hash(Row* self, Keys* keys)
{
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto column = list_at(Key, link)->column;
		// fixed or variable type
		if (column->type_size > 0) {
			hash = hash_murmur3_32(row_at(self, column->order), column->type_size, hash);
		} else {
			uint8_t* pos = row_at(self, column->order);
			Str str;
			json_read_string(&pos, &str);
			hash = hash_murmur3_32(str_u8(&str), str_size(&str), hash);
		}
	}
	return hash;
}

hot static inline Row*
row_copy(Row* self)
{
	auto size = row_size(self);
	auto row  = am_malloc(size);
	memcpy(row, self, size);
	return row;
}
