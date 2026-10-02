
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
#include <amelie_compiler>
#include <amelie_backend.h>

void
streams_init(Streams* self, Task* task, Client* client)
{
	self->streams       = NULL;
	self->streams_count = 0;
	self->task          = task;
	self->client        = client;
	list_init(&self->ready);
	buf_init(&self->key);
	event_init(&self->notify);
	iov_init(&self->iov);
}

void
streams_free(Streams* self)
{
	if (self->streams)
	{
		for (auto i = 0; i < self->streams_count; i++)
			stream_free(&self->streams[i]);
		am_free(self->streams);
		self->streams = NULL;
	}
	buf_free(&self->key);
	iov_free(&self->iov);
}

static void
streams_create_key(Streams* self, Parts* parts, Str* key)
{
	auto table   = table_of(parts->arg->rel);
	auto primary = table_primary(table);
	if (primary->keys.count > 1)
		error("stream: compound table keys are not support");

	Local local;
	local_init(&local);

	// read key and convert
	auto column = keys_at(&primary->keys, 0)->column;
	Value value;
	value_init(&value);
	defer(value_free, &value);
	parse_value_string(&local, column, &value, key);

	// create row
	row_create_key(&self->key, &primary->keys, &value, 1);
}

void
streams_create(Streams* self, Parts* parts, Timeline* timeline, Str* key_str)
{
	// prepare key
	Row* key = NULL;
	if (! str_empty(key_str))
	{
		streams_create_key(self, parts, key_str);
		key = (Row*)self->key.start;
	}

	self->streams_count = parts->list_count;
	self->streams = am_malloc(sizeof(Stream) * self->streams_count);

	// prepare streams per partitions and set as ready
	auto at = 0;
	list_foreach(&parts->list)
	{
		auto part = list_at(Part, link);
		auto stream = &self->streams[at];
		stream_init(stream, self, part, timeline);

		event_attach(&stream->on_complete);
		event_set_parent(&stream->on_complete, &self->notify);
		event_attach(&stream->on_cancel);
		event_set_parent(&stream->on_cancel, &self->notify);

		stream->key   = key;
		stream->ready = true;
		list_append(&self->ready, &stream->link);
		at++;
	}
}

hot static void
streams_main(Streams* self)
{
	auto client = self->client;

	// prepare client disconnect event
	Event eof;
	event_init(&eof);
	event_set_parent(&eof, &self->notify);
	poll_read_start(&client->tcp.fd, &eof);
	defer(poll_read_stop, &client->tcp.fd);

	auto iov = &self->iov;
	for (;;)
	{
		// send ready
		while (! list_empty(&self->ready))
		{
			auto stream = container_of(list_pop(&self->ready), Stream, link);
			stream->ready = false;
			list_init(&stream->link);
			buf_reset(&stream->data);
			task_send(stream->part_task, &stream->msg);
		}
		list_init(&self->ready);

		// wait
		event_wait(&self->notify, -1);

		// client disconnect
		if (unlikely(eof.signal))
			break;

		// collect results
		iov_reset(iov);
		auto shutdown = false;
		for (auto i = 0; i < self->streams_count; i++)
		{
			auto stream = &self->streams[i];
			if (! stream->on_complete.signal)
				continue;

			stream->on_complete.signal = false;
			if (stream->shutdown)
				shutdown = true;

			if (! buf_empty(&stream->data))
				iov_add_buf(iov, &stream->data);

			stream->ready = true;
			list_append(&self->ready, &stream->link);
		}
		if (unlikely(shutdown))
			break;

		// batch send
		if (! iov_empty(iov))
			tcp_write(&client->tcp, iov_pointer(iov), iov->iov_count);
	}
}

static void
streams_shutdown(Streams* self)
{
	auto pending = false;
	for (auto i = 0; i < self->streams_count; i++)
	{
		auto stream = &self->streams[i];
		if (stream->ready)
			continue;
		task_send(stream->part_task, &stream->msg_cancel);
		pending = true;
	}
	if (! pending)
		return;

	// wait for completion
	for (auto i = 0; i < self->streams_count; i++)
	{
		auto stream = &self->streams[i];
		if (stream->ready)
			continue;
		event_wait(&stream->on_complete, -1);
		event_wait(&stream->on_cancel, -1);
		stream->ready = true;
		list_append(&self->ready, &stream->link);
	}
}

void
streams_run(Streams* self)
{
	// relay streams data to the client
	error_catch( streams_main(self) );

	// cancel and ensure everyone finished
	cancel_pause();
	streams_shutdown(self);
	cancel_resume();
}
