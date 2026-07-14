
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

static inline void
topic_free(Topic* self, bool drop)
{
	unused(drop);
	topic_config_free(self->config);
	am_free(self);
}

static inline void
topic_show(Topic* self, Buf* buf, int flags)
{
	topic_config_write(self->config, buf, flags);
}

static inline Topic*
topic_allocate(TopicConfig* config)
{
	auto self = (Topic*)am_malloc(sizeof(Topic));
	self->config = topic_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TOPIC);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_id(rel, &self->config->id);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)topic_show);
	rel_set_free(rel, (RelFree)topic_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
topic_create(Catalog*     self,
             Tr*          tr,
             TopicConfig* config,
             bool         if_not_exists)
{
	// PERM_CREATE_TOPIC
	check_user(tr, PERM_CREATE_TOPIC);

	// make sure topic does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}

	// create topic
	auto topic = topic_allocate(config);
	rels_create(&self->rels, tr, &topic->rel);
	return true;
}
