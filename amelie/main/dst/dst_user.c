
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
	self->id     = id;
	self->client = NULL;
	self->dst    = dst;
	endpoint_init(&self->endpoint);
	dst_log_init(&self->log);
	for (auto i = 0; i < DST_REL_MAX; i++)
	{
		auto rel = &self->rels[i];
		dst_rel_init(rel, i);
	}
}

void
dst_user_free(DstUser* self)
{
	if (self->client)
		client_free(self->client);
	dst_log_free(&self->log);
	endpoint_free(&self->endpoint);
	for (auto i = 0; i < DST_REL_MAX; i++)
		dst_rel_free(&self->rels[i]);
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

void
dst_user_create(DstUser* self)
{
	// prepare relations
	auto keys = self->dst->opt_keys.integer * 2;
	hashtable_create(&self->rels[DST_REL_TABLE].state, keys);
	hashtable_create(&self->rels[DST_REL_TABLE_VECTOR].state, keys);

	// create relations
	char* ddl[] =
	{
		"CREATE TABLE dst_table (id int primary key using hash, state bigint)",
		"CREATE TABLE dst_table_vector (id int primary key, state vector(4))",
		"CREATE TOPIC dst_topic",
		"CREATE SUBSCRIPTION dst_sub_table ON dst_table",
		//"CREATE SUBSCRIPTION dst_sub_table_vector ON dst_table_vector",
		"CREATE SUBSCRIPTION dst_sub_topic ON dst_topic",
		 NULL
	};
	for (auto i = 0; ddl[i]; i++)
		dst_execute(self->dst, self->client, "{s}", ddl[i]);
}
