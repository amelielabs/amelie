
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compute.h>

static inline void
streaming_start(Link* self)
{
	auto client = self->client;
	auto reply = &client->reply;
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6, 0);
	buf_write(buf, "Cache-Control: no-cache\r\n", 25);
	buf_write(buf, "Connection: keep-alive\r\n", 24);
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);
}

static Table*
streaming_prepare(Link* self, StreamPos* pos)
{
	auto req = &self->req;

	// find relation
	auto rel_name = &req->endpoint.rel;
	auto rel = catalog_find(&req->db->catalog, REL_UNDEF, rel_name, false);
	if (! rel)
		error("stream: relation {str} not found", rel_name);
	if (rel->type != REL_TABLE)
		error("stream: relation {str} cannot be used for streaming",
		      rel_name);

	// prepare streams (one per partition)
	auto id = &req->endpoint.id;
	if (! str_empty(id))
		if (stream_pos_read(pos, id) == -1)
			error("stream: invalid id");

	auto table = table_of(rel);
	return table;
}

hot static void
streaming_main(Link* self, StreamPos* pos, Part* part)
{
	Stream stream;
	stream_init(&stream, pos, part);
	defer(stream_free, &stream);

	// client disconnect event
	auto client = self->client;
	Event eof;
	event_init(&eof);
	poll_read_start(&client->tcp.fd, &eof);
	defer(poll_read_stop, &client->tcp.fd);

	// route stream event
	Event event;
	event_init(&event);
	event_set_parent(&stream.sub.event, &event);
	event_set_parent(&eof, &event);

	// main
	auto req = &self->req;
	for (;;)
	{
		// collect events
		auto rc = stream_next(&stream);
		if (rc > 0)
		{
			tcp_write_buf(&client->tcp, &stream.data);
			continue;
		}
		if (unlikely(rc == -1))
			break;

		// attach and wait (resume, eof or cancel)
		stream_attach(&stream);

		request_unlock(req);

		// wait
		auto on_cancel = error_catch
		(
			event.signal = false;
			event_wait(&event, -1)
		);

		request_lock(req, LOCK_SHARED); // todo: cancellation point

		if (stream_detach(&stream))
			break;

		if (unlikely(on_cancel || eof.signal))
			break;
	}
}

void
streaming(Link* self)
{
	StreamPos pos;
	stream_pos_init(&pos);

	// find relation and read id
	Table* table = NULL;
	auto on_error = error_catch
	(
		table = streaming_prepare(self, &pos);
	);

	auto req = &self->req;
	if (on_error)
	{
		// 400 Bad Request
		buf_reset(req->output.buf);
		output_error(&req->output, &am_self()->error);
		client_400(self->client, req->output.buf);
		return;
	}

	// start
	streaming_start(self);
	streaming_main(self, &pos, table->part);
}
