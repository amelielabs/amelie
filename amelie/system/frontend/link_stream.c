
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
link_stream_create(Link* self, Streams* streams)
{
	// find relation
	auto api = self->api;
	auto rel = catalog_find(&share()->db->catalog, REL_UNDEF, &api->rel_user, &api->rel, false);
	if (! rel)
		error("stream: relation {str}.{str} not found",
		      &api->rel_user, &api->rel);

	// table or layer
	Timeline* timeline;
	Part*     part;
	if (rel->type == REL_TABLE)
	{
		auto table = table_of(rel);
		timeline = &table->timelines.main;
		part     =  table->part;
	} else
	if (rel->type == REL_LAYER)
	{
		auto layer = layer_of(rel);
		timeline = &layer->config->timeline;
		part     =  layer->table->part;
	} else {
		error("stream: relation {str}.{str} cannot be used for streaming",
		      rel->user, rel->name);
	}

	// prepare streams (one per partition)
	auto id = &self->portal.endpoint.id.string;
	if (str_empty(id))
		streams_create(streams, part, timeline);
	else
		streams_create_for(streams, part, timeline, id);
}

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
	// prepare streams (one per partition)
	Streams streams;
	streams_init(&streams, am_task, self->client);
	defer(streams_free, &streams);
	auto on_error = error_catch
	(
		link_stream_create(self, &streams);
	);

	auto portal = &self->portal;
	if (on_error)
	{
		// 400 Bad Request
		buf_reset(portal->output.buf);
		output_error(&portal->output, &am_self()->error);
		client_400(self->client, portal->output.buf);
		return;
	}

	// release catalog lock
	portal_unlock(portal);

	// start sse streaming
	link_stream_begin(self);

	// start streaming data to the client
	streams_run(&streams);
}
