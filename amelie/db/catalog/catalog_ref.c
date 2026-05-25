
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

Rel*
catalog_cdc_ref(Catalog* self, User* user, Str* rel_user, Str* rel,
                Uuid**   id)
{
	auto ref = catalog_find(self, REL_UNDEF, rel_user, rel, true);
	auto ref_match = ref;

	// match existing subscription relation
	if (ref->type == REL_SUBSCRIPTION)
	{
		auto sub = sub_of(ref);
		ref_match = catalog_find_by(self, REL_UNDEF, &sub->on_id, true);
	}

	// check permission to create subscription on the relation
	user_check_permission(user, ref_match, PERM_CREATE_SUBSCRIPTION);

	// reference relation being used for cdc
	switch (ref_match->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref_match);
		table->part_arg.cdc++;
		*id = &table->config->id;
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(ref_match);
		clone->cdc++;
		*id = &clone->config->id;
		break;
	}
	case REL_TOPIC:
	{
		auto topic = topic_of(ref_match);
		topic->cdc++;
		*id = &topic->config->id;
		break;
	}
	default:
		error("relation '{str}': cannot be used for cdc", rel);
		break;
	}
	return ref;
}

void
catalog_cdc_unref(Catalog* self, Uuid* id)
{
	auto ref = catalog_find_by(self, REL_UNDEF, id, false);
	if (! ref)
		return;

	// reference relation being used for cdc
	switch (ref->type) {
	case REL_TABLE:
	{
		auto table = table_of(ref);
		table->part_arg.cdc--;
		assert(table->part_arg.cdc >= 0);
		break;
	}
	case REL_CLONE:
	{
		auto clone = clone_of(ref);
		clone->cdc--;
		assert(clone->cdc >= 0);
		break;
	}
	case REL_TOPIC:
	{
		auto topic = topic_of(ref);
		topic->cdc--;
		assert(topic->cdc >= 0);
		break;
	}
	default:
		abort();
	}
}
