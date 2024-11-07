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
	// column offsets start from 0
	register uint32_t offset;
	if (self->size_factor == 0)
		offset = sizeof(uint8_t) + self->u8[1 + column];
	else
	if (self->size_factor == 1)
		offset = sizeof(uint16_t) + self->u16[1 + column];
	else
		offset = sizeof(uint32_t) + self->u32[1 + column];
	return (uint8_t*)self + sizeof(Row) + offset;
}

hot static inline Row*
row_allocate(int columns, int data_size)
{
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

#if 0
Row* row_alter_add(Row*, Buf*);
Row* row_alter_drop(Row*, int);
#endif
