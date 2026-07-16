
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_dst.h>

DstUser*
dst_user_allocate(Dst* dst, uint64_t id)
{
	auto self = (DstUser*)am_malloc(sizeof(DstUser));
	self->id         = id;
	self->client     = NULL;
	self->dst        = dst;
	self->rels_seq   = 0;
	self->rels_count = 0;
	endpoint_init(&self->endpoint);
	dst_log_init(&self->log);
	list_init(&self->rels);
	list_init(&self->link);
	return self;
}

void
dst_user_free(DstUser* self)
{
	if (self->client)
		client_free(self->client);
	dst_log_free(&self->log);
	endpoint_free(&self->endpoint);
	list_foreach_safe(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		dst_rel_free(rel);
	}
	am_free(self);
}

void
dst_user_connect(DstUser* self)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{str}/env", opt_string_of(&self->dst->opt_dir));

	char name[32];
	format(name, sizeof(name), "user_{d}", self->id);

	// connect 
	auto endpoint = &self->endpoint;
	opt_string_set_cstr(&endpoint->path, path);
	opt_string_set_cstr(&endpoint->user, name);
	opt_string_set_raw(&endpoint->content_type, "text/plain", 10);
	opt_string_set_raw(&endpoint->accept, "application/json", 16);

	self->client = client_allocate();
	client_set_endpoint(self->client, endpoint);
	client_connect(self->client);
}

void
dst_user_close(DstUser* self)
{
	if (! self->client)
		return;
	client_free(self->client);
	self->client = NULL;
}

DstRel*
dst_user_create(DstUser* self, int type)
{
	// set next id
	auto id = self->rels_seq++;

	// create object
	auto rel = dst_rel_allocate(NULL, id, type, self->dst->opt_keys.integer);
	list_append(&self->rels, &rel->link);
	self->rels_count++;

	// create relation
	switch (type) {
	case DST_REL_TABLE:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TABLE table_{u64} (id int primary key using hash, state bigint)",
		            id);
		break;
	}
	case DST_REL_TABLE_VECTOR:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TABLE table_vector_{u64} (id int primary key, state vector(4))",
		            id);
		break;
	}
	case DST_REL_TOPIC:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TOPIC topic_{u64}",
		            id);
		break;
	}
	default:
	{
		abort();
		break;
	}
	}
	return rel;
}

DstRel*
dst_user_create_for(DstUser* self, DstRel* parent, int type)
{
	// set next id
	auto id = self->rels_seq++;

	// create object
	auto rel = dst_rel_allocate(parent, id, type, self->dst->opt_keys.integer);
	list_append(&self->rels, &rel->link);
	self->rels_count++;

	// create relation
	if (type == DST_REL_SUBSCRIPTION)
	{
		switch (parent->type) {
		case DST_REL_TABLE:
			dst_execute(self->dst, self->client,
			            "CREATE SUBSCRIPTION sub_{u64}_{u64} ON table_{u64}",
			            parent->id, id, parent->id);
			break;
		case DST_REL_TABLE_VECTOR:
			abort();
			break;
		case DST_REL_TOPIC:
			dst_execute(self->dst, self->client,
			            "CREATE SUBSCRIPTION sub_{u64}_{u64} ON topic_{u64}",
			            parent->id, id, parent->id);
			break;
		default:
			abort();
			break;
		}

		list_append(&parent->subs, &rel->link_parent);
		parent->subs_count++;
	} else
	if (type == DST_REL_CLONE)
	{
		assert(parent->type == DST_REL_TABLE);

		dst_execute(self->dst, self->client,
		            "CREATE CLONE clone_{u64}_{u64} OF table_{u64}",
		            parent->id, id, parent->id);

		dst_rel_copy(rel, parent);

		list_append(&parent->clones, &rel->link_parent);
		parent->clones_count++;
	} else
	{
		abort();
	}
	return rel;
}

void
dst_user_drop(DstUser* self, DstRel* rel)
{
	switch (rel->type) {
	case DST_REL_TABLE:
		dst_execute(self->dst, self->client,
		            "DROP TABLE table_{u64} CASCADE",
		            rel->id);
		break;
	case DST_REL_TABLE_VECTOR:
		dst_execute(self->dst, self->client,
		            "DROP TABLE table_vector_{u64} CASCADE",
		            rel->id);
		break;
	case DST_REL_CLONE:
		dst_execute(self->dst, self->client,
		            "DROP CLONE clone_{u64}_{u64} CASCADE",
		            rel->parent->id, rel->id);
		list_unlink(&rel->link_parent);
		rel->parent->clones_count--;
		break;
	case DST_REL_TOPIC:
		dst_execute(self->dst, self->client,
		            "DROP TOPIC topic_{u64} CASCADE",
		            rel->id);
		break;
	case DST_REL_SUBSCRIPTION:
		dst_execute(self->dst, self->client,
		            "DROP SUBSCRIPTION sub_{u64}_{u64} CASCADE",
		            rel->parent->id, rel->id);
		list_unlink(&rel->link_parent);
		rel->parent->subs_count--;
		break;
	}

	// free subscriptions
	list_foreach_safe(&rel->subs)
	{
		auto sub = list_at(DstRel, link_parent);
		list_unlink(&sub->link);
		self->rels_count--;
		assert(self->rels_count >= 0);
		dst_rel_free(sub);
	}

	// free clones
	list_foreach_safe(&rel->clones)
	{
		auto clone = list_at(DstRel, link_parent);
		list_unlink(&clone->link);
		self->rels_count--;
		assert(self->rels_count >= 0);
		dst_rel_free(clone);
	}

	list_unlink(&rel->link);
	self->rels_count--;
	assert(self->rels_count >= 0);
	dst_rel_free(rel);
}
