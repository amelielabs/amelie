
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_limit(Catalog* self, Tr* tr, RelType type, int limit_id)
{
	if (!tr->local || !tr->local->limits)
		return;

	auto limits = tr->local->limits;
	if (! limits_is_set(limits, limit_id))
		return;

	auto user = user_of(tr->user);
	auto rels = &self->rels;
	if (type == REL_USER)
		rels = &self->users;

	auto count = rels_count(rels, type, &user->config->name) + 1;
	if (unlikely(! limits_check(limits, limit_id, count)))
		error("{s} '{str}': {s}s limit reached",
		      user->config->agent ? "agent" :"user", &user->config->name,
		      rel_type_of(type));
}
