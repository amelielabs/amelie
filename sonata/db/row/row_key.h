#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct RowKey RowKey;

struct RowKey
{
	Row*     row;
	uint16_t key[];
} packed;

always_inline static inline int
row_key_size(Keys* keys)
{
	return sizeof(RowKey) + keys->list_count * sizeof(uint16_t);
}

always_inline static inline uint8_t*
row_key(RowKey* self, int order)
{
	return row_data(self->row) + self->key[order];
}

hot static inline void
row_key_create(RowKey* self, Row* row, Keys* keys)
{
	self->row = row;
	uint8_t* data = row_data(row);
	list_foreach(&keys->list)
	{
		const auto key = list_at(Key, link);

		// get column data
		uint8_t* pos = data;
		array_find(&pos, key->column->order);

		// find key path and validate data type
		key_find(key, &pos);

		// validate key offset
		uintptr_t offset = pos - data;
		if (unlikely(offset > UINT16_MAX))
			error("key offset exceeds 16bit");

		// set key offset
		self->key[key->order] = offset;
	}
}

hot static inline void
row_key_create_and_hash(RowKey* self, Row* row, Keys* keys, uint32_t* hash)
{
	self->row = row;
	uint8_t* data = row_data(row);
	list_foreach(&keys->list)
	{
		const auto key = list_at(Key, link);

		// get column data
		uint8_t* pos = data;
		array_find(&pos, key->column->order);

		// find key path and validate data type
		key_find(key, &pos);

		// validate key offset
		uintptr_t offset = pos - data;
		if (unlikely(offset > UINT16_MAX))
			error("key offset exceeds 16bit");

		// set key offset
		self->key[key->order] = offset;

		// hash
		*hash = key_hash(*hash, row_key(self, key->order));
	}
}

hot static inline int
compare(Keys* self, RowKey* a, RowKey* b)
{
	list_foreach(&self->list)
	{
		const auto key = list_at(Key, link);
		int rc;
		if (key->type == TYPE_INT)
			rc = data_compare_integer(row_key(a, key->order),
			                          row_key(b, key->order));
		else
			rc = data_compare_string(row_key(a, key->order),
			                         row_key(b, key->order));
		if (rc != 0)
			return rc;
	}
	return 0;
}
