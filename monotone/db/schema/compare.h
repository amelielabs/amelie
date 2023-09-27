#pragma once

//
// monotone
//
// SQL OLTP database
//

hot static inline int
compare_n(Schema* schema, int count, Row* a, Row* b)
{
	assert(count <= schema->key_count);

	auto key = schema->key;
	for (; key && count > 0; key = key->next_key)
	{
		uint8_t* a_data = row_key(a, schema, key->order_key);
		uint8_t* b_data = row_key(b, schema, key->order_key);
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
compare(Schema* schema, Row* a, Row* b)
{
	return compare_n(schema, schema->key_count, a, b);
}
