#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

hot static inline int
compare_n(Keys* self, int count, Row* a, Row* b)
{
	list_foreach(&self->list)
	{
		if (unlikely(count == 0))
			break;
		auto key    = list_at(Key, link);
		auto a_data = row_key(a, self, key->order);
		auto b_data = row_key(b, self, key->order);
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
compare(Keys* self, Row* a, Row* b)
{
	return compare_n(self, self->list_count, a, b);
}
