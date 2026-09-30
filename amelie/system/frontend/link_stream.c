
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
link_stream_begin(Link* self)
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
link_stream(Link* self)
{
	// find table
	auto api = self->api;
	auto table = catalog_find_table(&share()->db->catalog, &api->rel_user, &api->rel, true);

	// prepare tails (one per partition)
	Tails tails;
	tails_init(&tails, am_task, self->client);
	tails_create(&tails, &table->parts, &self->portal.endpoint.id.string);
	defer(tails_free, &tails);

	// release catalog lock
	portal_unlock(&self->portal);

	// start sse streaming
	link_stream_begin(self);

	// start sending tails data to the client
	tails_run(&tails);
}
