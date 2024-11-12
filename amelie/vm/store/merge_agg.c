
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

hot static void
merge_agg_row(Set* self, Value* row)
{
	// find or create row by key
	auto src = set_get_or_add(self, &row[self->count_columns]);

	// merge aggregate s and free keys
	for (int col = 0; col < self->count_columns; col++)
	{
		if (src[col].type == VALUE_NULL) {
			value_move(&src[col], &row[col]);
		} else {
			assert(src[col].type == VALUE_AGG);
			assert(row[col].type == VALUE_AGG);
			agg_merge(&src[col].agg, &row[col].agg);
		}
	}
	for (int key = 0; key < self->count_keys; key++)
		value_free(&row[self->count_columns + key]);
}

hot void
merge_agg(Value** list, int list_count)
{
	if (list_count <= 1)
		return;

	auto self = (Set*)list[0]->store;
	for (int i = 1; i < list_count; i++)
	{
		auto set = (Set*)list[i]->store;
		if (set->count == 0)
			continue;

		for (int pos = 0; pos < set->hash.hash_size; pos++)
		{
			auto ref = &set->hash.hash[pos];
			if (ref->row == UINT32_MAX)
				continue;
			auto row = set_row(set, ref->row);
			merge_agg_row(self, row);
		}

		set->count = 0;
		set->count_rows = 0;
		store_free(&set->store);
		value_set_null(list[i]);
	}
}
