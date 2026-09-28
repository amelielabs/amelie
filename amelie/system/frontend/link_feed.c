
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
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_backend.h>
#include <amelie_frontend.h>

static inline void
link_feed_begin(Link* self)
{
	auto client = self->client;
	auto reply = &client->reply;
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6, 0);
	buf_write(buf, "Cache-Control: no-cache\r\n", 25);
	buf_write(buf, "Connection: keep-alive\r\n", 24);
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);
}

void
link_feed(Link* self)
{
	// find table
	auto api = self->api;
	auto table = catalog_find_table(&share()->db->catalog, &api->rel_user, &api->rel, true);

	// prepare feeds (one per partition)
	Feeds feeds;
	feeds_init(&feeds, am_task, self->client);
	feeds_create(&feeds, &table->parts, &self->portal.endpoint.id.string);

	// release catalog lock
	portal_unlock(&self->portal);

	// begin SSE
	link_feed_begin(self);

	// start sending feeds data to the client
	feeds_run(&feeds);
}
