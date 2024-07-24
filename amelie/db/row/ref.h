#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Ref Ref;

struct Ref
{
	Row*     row;
	uint16_t key[];
} packed;

always_inline static inline uint8_t*
ref_key(Ref* self, int order)
{
	return row_data(self->row) + self->key[order];
}

always_inline static inline int
ref_size(Keys* keys)
{
	return sizeof(Ref) + keys->list_count * sizeof(uint16_t);
}

hot static inline void
ref_create(Ref* self, Row* row, Keys* keys)
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

hot static inline int
compare(Keys* self, Ref* a, Ref* b)
{
	list_foreach(&self->list)
	{
		const auto key = list_at(Key, link);
		int rc;
		if (key->type == TYPE_INT)
			rc = data_compare_integer(ref_key(a, key->order),
			                          ref_key(b, key->order));
		else
		if (key->type == TYPE_TIMESTAMP ||
		    key->type == TYPE_TIMESTAMPTZ)
			rc = data_compare_timestamp(ref_key(a, key->order),
			                            ref_key(b, key->order));
		else
			rc = data_compare_string(ref_key(a, key->order),
			                         ref_key(b, key->order));
		if (rc != 0)
			return rc;
	}
	return 0;
}
