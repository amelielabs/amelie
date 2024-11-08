#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

hot static inline int
compare(Keys* self, Row* a, Row* b)
{
	(void)self;
	(void)a;
	(void)b;
#if 0
	list_foreach(&self->list)
	{
		const auto key = list_at(Key, link);
		/*
		int rc;
		if (key->type == TYPE_INT)
			rc = data_compare_integer(ref_key(a, key->order),
			                          ref_key(b, key->order));
		else
		if (key->type == TYPE_TIMESTAMP)
			rc = data_compare_timestamp(ref_key(a, key->order),
			                            ref_key(b, key->order));
		else
			rc = data_compare_string(ref_key(a, key->order),
			                         ref_key(b, key->order));
		if (rc != 0)
			return rc;
			*/
	}
#endif
	return 0;
}
