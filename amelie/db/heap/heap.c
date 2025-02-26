
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>

#if 0
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
#endif

