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
	union {
		uint8_t  u8[1];
		uint16_t u16[1];
		uint32_t u32[1];
	};
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
		*self->u8 = total;
	else
	if (self->size_factor == 1)
		*self->u16 = total;
	else
		*self->u32 = total;
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
		return *self->u8;
	if (self->size_factor == 1)
		return *self->u16;
	return *self->u32;
}

always_inline hot static inline void*
row_at(Row* self, int column)
{
	register uint32_t offset;
	if (self->size_factor == 0)
		offset = self->u8[1 + column];
	else
	if (self->size_factor == 1)
		offset = self->u16[1 + column];
	else
		offset = self->u32[1 + column];
	if (offset == 0)
		return NULL;
	return (uint8_t*)self + offset;
}

always_inline hot static inline void*
row_data(Row* self, int columns)
{
	register uint32_t offset = columns + 1;
	if (self->size_factor == 0)
		offset *= sizeof(uint8_t);
	else
	if (self->size_factor == 1)
		offset *= sizeof(uint16_t);
	else
		offset *= sizeof(uint16_t);
	return (uint8_t*)self + sizeof(Row) + offset;
}

always_inline hot static inline void
row_set(Row* self, int column, int offset)
{
	if (self->size_factor == 0)
		self->u8[1 + column]  = offset;
	else
	if (self->size_factor == 1)
		self->u16[1 + column] = offset;
	else
		self->u32[1 + column] = offset;
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

Row* row_alter_add(Row*, Buf*);
Row* row_alter_drop(Row*, int);
