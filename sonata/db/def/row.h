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
	uint8_t  unused: 6;
	uint32_t hash;
	uint8_t  size[];
	// size
	// data_index[]
	// data[]
} packed;

always_inline hot static inline int
row_data_size_meta(Def* def, int size_factor)
{
	// data size + index size
	return sizeof(Row) + (1 + size_factor) * (1 + def->key_count);
}

always_inline hot static inline uint8_t*
row_data(Row* self, Def* def)
{
	return (uint8_t*)self + row_data_size_meta(def, self->size_factor);
}

always_inline hot static inline Row*
row_of(Row* self, Def* def)
{
	// primary row (has no primary def pointer)
	if (! def->primary)
		return self;
	// secondary row
	return *(Row**)row_data(self, def);
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
row_data_size(Row* self, Def* def)
{
	// resolve primary row
	self = row_of(self, def);
	if (self->size_factor == 0)
		return row_data_1(self)[0];
	if (self->size_factor == 1)
		return row_data_2(self)[0];
	return row_data_4(self)[0];
}

always_inline hot static inline uint8_t*
row_key(Row* self, Def* def, int pos)
{
	assert(pos < def->key_count);
	uint32_t offset;
	if (self->size_factor == 0)
		offset = row_data_1(self)[1 + pos];
	else
	if (self->size_factor == 1)
		offset = row_data_2(self)[1 + pos];
	else
		offset = row_data_4(self)[1 + pos];
	// apply key offset to the primary row
	self = row_of(self, def);
	return row_data(self, def) + offset;
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
row_allocate(Def* def, int data_size)
{
	// calculate size factor
	int size_factor;
	if ((sizeof(Row) +
	    (sizeof(uint8_t) * (1 + def->key_count)) + data_size) <= UINT8_MAX)
		size_factor = 0;
	else
	if ((sizeof(Row) +
	    (sizeof(uint16_t) * (1 + def->key_count)) + data_size) <= UINT16_MAX)
		size_factor = 1;
	else
		size_factor = 3;

	// allocate row
	int size = row_data_size_meta(def, size_factor) + data_size;
	Row* self = so_malloc(size);
	self->size_factor = size_factor;
	self->hash        = 0;
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

Row* row_create(Def*, bool, uint8_t**);
Row* row_create_secondary(Def*, bool, Row*);

uint32_t
row_hash(Def*, uint8_t**);
