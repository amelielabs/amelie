#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Row Row;

struct Row
{
	uint8_t size_factor: 2;
	uint8_t is_secondary: 1;
	uint8_t is_partial: 1;
	uint8_t flags: 3;
	uint8_t data[];
	//   reserved[]
	//   size
	//   data_index[]
	//   data[]
} packed;

always_inline hot static inline uint8_t*
row_data_1(Row* self, Key* key)
{
	return self->data + key->reserved;
}

always_inline hot static inline uint16_t*
row_data_2(Row* self, Key* key)
{
	return (uint16_t*)(self->data + key->reserved);
}

always_inline hot static inline uint32_t*
row_data_4(Row* self, Key* key)
{
	return (uint32_t*)(self->data + key->reserved);
}

always_inline hot static inline int
row_data_size(Row* self, Key* key)
{
	if (self->size_factor == 0)
		return row_data_1(self, key)[0];
	if (self->size_factor == 1)
		return row_data_2(self, key)[0];
	return row_data_4(self, key)[0];
}

always_inline hot static inline void
row_set_data_size(Row* self, Key* key, int data_size)
{
	if (self->size_factor == 0)
		row_data_1(self, key)[0] = data_size;
	else
	if (self->size_factor == 1)
		row_data_2(self, key)[0] = data_size;
	else
		row_data_4(self, key)[0] = data_size;
}

always_inline hot static inline int
row_size_meta(Key* key, int size_factor)
{
	// reserved + data size + index size
	return sizeof(Row) + key->reserved + (1 + size_factor) * (1 + key->key_count);
}

always_inline hot static inline int
row_size(Row* self, Key* key)
{
	return row_size_meta(key, self->size_factor) + row_data_size(self, key);
}

always_inline hot static inline int
row_size_factor_of(Key* key, int data_size)
{
	int size_factor;
	if ((sizeof(Row) +
	    (sizeof(uint8_t) * (1 + key->key_count)) + data_size) <= UINT8_MAX)
		size_factor = 0;
	else
	if ((sizeof(Row) +
	    (sizeof(uint16_t) * (1 + key->key_count)) + data_size) <= UINT16_MAX)
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
row_data(Row* self, Key* key)
{
	return (uint8_t*)self + row_size_meta(key, self->size_factor);
}

always_inline hot static inline uint8_t*
row_key(Row* self, Key* key, int pos)
{
	assert(pos < key->key_count);
	uint32_t offset;
	if (self->size_factor == 0)
		offset = row_data_1(self, key)[1 + pos];
	else
	if (self->size_factor == 0)
		offset = row_data_2(self, key)[1 + pos];
	else
		offset = row_data_4(self, key)[1 + pos];
	return row_data(self, key) + offset;
}

always_inline hot static inline void
row_key_set_index(Row* self, Key* key, int order, uint32_t offset)
{
	if (self->size_factor == 0)
		row_data_1(self, key)[1 + order] = offset;
	else
	if (self->size_factor == 1)
		row_data_2(self, key)[1 + order] = offset;
	else
		row_data_4(self, key)[1 + order] = offset;
}

hot static inline Row*
row_allocate(Key* key, int data_size)
{
	// calculate size factor
	int size_factor;
	size_factor = row_size_factor_of(key, data_size);

	// allocate row
	int size = row_size_meta(key, size_factor) + data_size;
	Row* self = mn_malloc(size);
	self->size_factor = size_factor;
	self->is_secondary = false;
	self->is_partial   = false;
	self->flags        = 0;

	// set data size
	row_set_data_size(self, key, data_size);
	return self;
}

static inline void
row_free(Row* self)
{
	mn_free(self);
}

Row* row_create(Key*, uint8_t*, int);
