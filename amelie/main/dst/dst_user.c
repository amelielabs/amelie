
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

void
dst_user_init(DstUser* self, Dst* dst, int id)
{
	self->id         = id;
	self->client     = NULL;
	self->dst        = dst;
	self->rels_count = 0;
	endpoint_init(&self->endpoint);
	dst_log_init(&self->log);
	list_init(&self->rels);
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

static void
dst_user_create_rel(DstUser* self, int id, int type)
{
	// create object
	auto rel = dst_rel_allocate(id, type, self->dst->opt_keys.integer);
	list_append(&self->rels, &rel->link);
	self->rels_count++;

	// create relation
	switch (type) {
	case DST_REL_TABLE:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TABLE table_{u64} (id int primary key using hash, state bigint)",
		            id);
		dst_execute(self->dst, self->client,
		            "CREATE SUBSCRIPTION table_sub_{u64} ON table_{u64}",
		            id, id);
		break;
	}
	case DST_REL_TABLE_VECTOR:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TABLE table_vector_{u64} (id int primary key, state vector(4))",
		            id);
		/*
		dst_execute(self->dst, self->client,
		            "CREATE SUBSCRIPTION table_vector_sub_{u64} ON table_vector_{u64}",
		            id, id);
		*/
		break;
	}
	case DST_REL_TOPIC:
	{
		dst_execute(self->dst, self->client,
		            "CREATE TOPIC topic_{u64}",
		            id);
		dst_execute(self->dst, self->client,
		            "CREATE SUBSCRIPTION topic_sub_{u64} ON topic_{u64}",
		            id, id);
		break;
	}
	}
}

void
dst_user_create(DstUser* self)
{
	// table
	dst_user_create_rel(self, 0, DST_REL_TABLE);

	// table vector
	dst_user_create_rel(self, 1, DST_REL_TABLE_VECTOR);

	// topic
	dst_user_create_rel(self, 2, DST_REL_TOPIC);
}
