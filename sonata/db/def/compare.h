#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

hot static inline int
compare_n(Def* self, int count, Row* a, Row* b)
{
	assert(count <= self->key_count);
	auto key = self->key;
	for (; key && count > 0; key = key->next)
	{
		uint8_t* a_data = row_key(a, self, key->order);
		uint8_t* b_data = row_key(b, self, key->order);
		int rc;
		if (key->type == TYPE_STRING)
			rc = data_compare_string(a_data, b_data);
		else
			rc = data_compare_integer(a_data, b_data);
		if (rc != 0)
			return rc;
		count--;
	}
	return 0;
}

hot static inline int
compare(Def* self, Row* a, Row* b)
{
	return compare_n(self, self->key_count, a, b);
}
