#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Row Row;

struct Row
{
	uint8_t size_factor: 2;
	uint8_t unused: 6;
	uint8_t data[];
} packed;

always_inline hot static inline int
row_size(Row* self)
{
	if (self->size_factor == 0)
		return *self->data;
	if (self->size_factor == 1)
		return *((uint16_t*)self->data);
	return *((uint32_t*)self->data);
}

always_inline hot static inline uint8_t*
row_data(Row* self)
{
	return self->data + (1 + self->size_factor);
}

hot static inline Row*
row_allocate(int data_size)
{
	// calculate size factor
	int size_factor;
	if ((sizeof(Row) + sizeof(uint8_t) + data_size) <= UINT8_MAX)
		size_factor = 0;
	else
	if ((sizeof(Row) + sizeof(uint16_t) + data_size) <= UINT16_MAX)
		size_factor = 1;
	else
		size_factor = 3;

	// allocate row
	int size = sizeof(Row) + (1 + size_factor) + data_size;
	Row* self = so_malloc(size);
	self->size_factor = size_factor;
	self->unused      = 0;

	// set data size
	if (self->size_factor == 0)
		*self->data = data_size;
	else
	if (self->size_factor == 1)
		*(uint16_t*)self->data = data_size;
	else
		*(uint32_t*)self->data = data_size;
	return self;
}

always_inline static inline void
row_free(Row* self)
{
	so_free(self);
}

Row* row_create(Columns*, uint8_t**);
void row_create_hash(Keys*, uint32_t*, uint8_t**);

Row* row_alter_add(Row*, Buf*);
Row* row_alter_drop(Row*, int);
