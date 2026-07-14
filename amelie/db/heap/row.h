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
	// 16 bytes

	// version
	uint64_t timeline:    32;

	// row
	uint64_t columns:     16;
	uint64_t size_factor: 2;
	uint64_t deleted:     1;
	uint64_t main:        1;
	uint64_t head:        1;

	// heap
	uint64_t bucket:      8;
	uint64_t free:        1;
	uint64_t reserved:    2;

	// heap version (64bit cut)
	uint64_t offset:      19;
	uint64_t prev:        19;
	uint64_t prev_offset: 19;
	uint64_t padding:     7;

	// data
	uint8_t  data[];
} packed;

always_inline hot static inline void
row_init(Row* self)
{
	auto init = (uint64_t*)self;
	init[0] = 0;
	init[1] = 0;
}

always_inline hot static inline void
row_prepare(Row*     self,
            bool     main,
            uint32_t timeline,
            int      columns,
            int      size_factor,
            int      size)
{
	self->timeline    = timeline;
	self->columns     = columns;
	self->size_factor = size_factor;
	self->deleted     = false;
	self->main        = main;
	self->head        = false;

	// set size
	switch (size_factor) {
	case 0:
		*self->data = size;
		break;
	case 1:
		*(uint16_t*)self->data = size;
		break;
	default:
		*(uint32_t*)self->data = size;
		break;

	}
}

always_inline hot static inline uint32_t
row_size(Row* self)
{
	switch (self->size_factor) {
	case 0:
		return *self->data;
	case 1:
		return *(uint16_t*)self->data;
	default:
		break;
	}
	return *(uint32_t*)self->data;
}

always_inline hot static inline void*
row_at(Row* self, int column)
{
	if (unlikely(column >= self->columns))
		return NULL;
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
row_column(Row* self, Column* column)
{
	return row_at(self, column->order);
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

always_inline hot static inline int
row_measure(int columns, int data_size, int* size_factor)
{
	// [row_header][size + index][data]
	int size_base = sizeof(Row) + data_size;
	int size = size_base + sizeof(uint8_t) * (1 + columns);
	if (size > UINT8_MAX)
	{
		size = size_base + sizeof(uint16_t) * (1 + columns);
		if (size > UINT16_MAX)
		{
			size = size_base + sizeof(uint32_t) * (1 + columns);
			*size_factor = 3;
		} else {
			*size_factor = 1;
		}
	} else {
		*size_factor = 0;
	}
	return size;
}

always_inline hot static inline uint32_t
row_hash(Row* self, Comparable* comparable)
{
	uint32_t  hash = 0;
	const int keys = comparable->keys_count;
	for (auto at = 0; at < keys; at++)
	{
		auto key = &((const ComparableKey*)comparable->keys.start)[at];
		auto pos = (uint8_t*)row_at(self, key->column);
		switch (key->type) {
		case COMPARE_I64:
		{
			// int64, timestamp
			hash = hash_murmur3_32(pos, sizeof(int64_t), hash);
			break;
		}
		case COMPARE_I32:
		{
			// int32
			hash = hash_murmur3_32(pos, sizeof(int32_t), hash);
			break;
		}
		case COMPARE_UUID:
		{
			// uuid
			hash = hash_murmur3_32(pos, sizeof(Uuid), hash);
			break;
		}
		case COMPARE_STR:
		{
			// string
			Str str;
			unpack_str(&pos, &str);
			hash = hash_murmur3_32(str_u8(&str), str_size(&str), hash);
			break;
		}
		default: __builtin_unreachable();
			break;
		}
	}
	return hash;
}
