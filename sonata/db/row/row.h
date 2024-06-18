#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Row Row;

struct Row
{
	uint8_t  size_factor: 2;
	uint8_t  ref: 1;
	uint8_t  unused: 5;
	uint8_t  size[];
	// size
	// data_index[]
	// data[]
} packed;

always_inline hot static inline int
row_data_size_meta(Keys* keys, int size_factor)
{
	// data size + index size
	return sizeof(Row) + (1 + size_factor) * (1 + keys->list_count);
}

always_inline hot static inline uint8_t*
row_data_of(Row* self, Keys* keys)
{
	return (uint8_t*)self + row_data_size_meta(keys, self->size_factor);
}

always_inline hot static inline uint8_t*
row_data(Row* self, Keys* keys)
{
	// primary row
	if (! self->ref)
		return row_data_of(self, keys);

	// secondary row (primary row reference)
	return row_data_of(*(Row**)row_data_of(self, keys), keys->primary);
}

always_inline hot static inline uint8_t*
row_data_1(Row* self)
{
	return self->size;
}

always_inline hot static inline uint16_t*
row_data_2(Row* self)
{
	return (uint16_t*)self->size;
}

always_inline hot static inline uint32_t*
row_data_4(Row* self)
{
	return (uint32_t*)self->size;
}

always_inline hot static inline int
row_data_size(Row* self, Keys* keys)
{
	// secondary row (primary row reference)
	if (self->ref)
		self = *(Row**)row_data_of(self, keys);
	if (self->size_factor == 0)
		return row_data_1(self)[0];
	if (self->size_factor == 1)
		return row_data_2(self)[0];
	return row_data_4(self)[0];
}

always_inline hot static inline uint8_t*
row_key(Row* self, Keys* keys, int pos)
{
	assert(pos < keys->list_count);
	uint32_t offset;
	if (self->size_factor == 0)
		offset = row_data_1(self)[1 + pos];
	else
	if (self->size_factor == 1)
		offset = row_data_2(self)[1 + pos];
	else
		offset = row_data_4(self)[1 + pos];
	return row_data(self, keys) + offset;
}

always_inline hot static inline void
row_key_set(Row* self, int order, uint32_t offset)
{
	if (self->size_factor == 0)
		row_data_1(self)[1 + order] = offset;
	else
	if (self->size_factor == 1)
		row_data_2(self)[1 + order] = offset;
	else
		row_data_4(self)[1 + order] = offset;
}

hot static inline Row*
row_allocate(Keys* keys, int data_size)
{
	// calculate size factor
	int size_factor;
	if ((sizeof(Row) +
	    (sizeof(uint8_t) * (1 + keys->list_count)) + data_size) <= UINT8_MAX)
		size_factor = 0;
	else
	if ((sizeof(Row) +
	    (sizeof(uint16_t) * (1 + keys->list_count)) + data_size) <= UINT16_MAX)
		size_factor = 1;
	else
		size_factor = 3;

	// allocate row
	int size = row_data_size_meta(keys, size_factor) + data_size;
	Row* self = so_malloc(size);
	self->size_factor = size_factor;
	self->ref         = false;
	self->unused      = 0;

	// set data size
	if (self->size_factor == 0)
		row_data_1(self)[0] = data_size;
	else
	if (self->size_factor == 1)
		row_data_2(self)[0] = data_size;
	else
		row_data_4(self)[0] = data_size;
	return self;
}

always_inline static inline void
row_free(Row* self)
{
	so_free(self);
}

Row* row_create(Keys*, uint8_t**);
Row* row_create_secondary(Keys*, Row*);

uint32_t
row_hash(Keys*, uint8_t**);
