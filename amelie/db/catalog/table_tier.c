
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>

hot static inline Tier*
table_tier_find(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		if (str_compare_case(&tier->name, name))
			return tier;
	}
	if (error_if_not_exists)
		error("tier '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}

Buf*
table_tier_list(Table* self, Str* ref, bool extended)
{
	unused(extended);
	auto buf = buf_create();
	errdefer_buf(buf);

	// show tier name on table
	if (ref)
	{
		auto tier = table_tier_find(self, ref, false);
		if (! tier)
			encode_null(buf);
		else
			tier_write(tier, buf, false);
		return buf;
	}

	// show tiers on table
	encode_array(buf);
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_write(tier, buf, false);
	}
	encode_array_end(buf);
	return buf;
}
