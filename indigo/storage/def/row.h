#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Row Row;

struct Row
{
	uint8_t size_factor: 2;
	uint8_t is_secondary: 1;
	uint8_t flags: 5;
	uint8_t data[];
	//   reserved[]
	//   size
	//   data_index[]
	//   data[]
} packed;

always_inline hot static inline uint8_t*
row_data_1(Row* self, Def* def)
{
	return self->data + def->reserved;
}

always_inline hot static inline uint16_t*
row_data_2(Row* self, Def* def)
{
	return (uint16_t*)(self->data + def->reserved);
}

always_inline hot static inline uint32_t*
row_data_4(Row* self, Def* def)
{
	return (uint32_t*)(self->data + def->reserved);
}

always_inline hot static inline int
row_data_size(Row* self, Def* def)
{
	if (self->size_factor == 0)
		return row_data_1(self, def)[0];
	if (self->size_factor == 1)
		return row_data_2(self, def)[0];
	return row_data_4(self, def)[0];
}

always_inline hot static inline void
row_set_data_size(Row* self, Def* def, int data_size)
{
	if (self->size_factor == 0)
		row_data_1(self, def)[0] = data_size;
	else
	if (self->size_factor == 1)
		row_data_2(self, def)[0] = data_size;
	else
		row_data_4(self, def)[0] = data_size;
}

always_inline hot static inline int
row_size_meta(Def* def, int size_factor)
{
	// reserved + data size + index size
	return sizeof(Row) + def->reserved + (1 + size_factor) * (1 + def->key_count);
}

always_inline hot static inline int
row_size(Row* self, Def* def)
{
	return row_size_meta(def, self->size_factor) + row_data_size(self, def);
}

always_inline hot static inline int
row_size_factor_of(Def* def, int data_size)
{
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
	return size_factor;
}

always_inline hot static inline void*
row_reserved(Row* self)
{
	return self->data;
}

always_inline hot static inline Row*
row_reserved_row(void* reserved)
{
	// flags
	return (Row*)((uint8_t*)reserved - sizeof(uint8_t));
}

always_inline hot static inline uint8_t*
row_data(Row* self, Def* def)
{
	return (uint8_t*)self + row_size_meta(def, self->size_factor);
}

always_inline hot static inline uint8_t*
row_key(Row* self, Def* def, int pos)
{
	assert(pos < def->key_count);
	uint32_t offset;
	if (self->size_factor == 0)
		offset = row_data_1(self, def)[1 + pos];
	else
	if (self->size_factor == 0)
		offset = row_data_2(self, def)[1 + pos];
	else
		offset = row_data_4(self, def)[1 + pos];
	return row_data(self, def) + offset;
}

always_inline hot static inline void
row_key_set_index(Row* self, Def* def, int order, uint32_t offset)
{
	if (self->size_factor == 0)
		row_data_1(self, def)[1 + order] = offset;
	else
	if (self->size_factor == 1)
		row_data_2(self, def)[1 + order] = offset;
	else
		row_data_4(self, def)[1 + order] = offset;
}

hot static inline Row*
row_allocate(Def* def, int data_size)
{
	// calculate size factor
	int size_factor;
	size_factor = row_size_factor_of(def, data_size);

	// allocate row
	int size = row_size_meta(def, size_factor) + data_size;
	Row* self = mn_malloc(size);
	self->size_factor = size_factor;
	self->is_secondary = false;
	self->flags        = 0;

	// set data size
	row_set_data_size(self, def, data_size);
	return self;
}

static inline void
row_free(Row* self)
{
	mn_free(self);
}

Row* row_create(Def*, uint8_t*, int);

uint32_t
row_hash(Def*, uint8_t*, int);
