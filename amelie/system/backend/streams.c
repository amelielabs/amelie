
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
	self->stream        = NULL;
	self->task          = task;
	self->client        = client;
	list_init(&self->ready);
	event_init(&self->notify);
	iov_init(&self->iov);
}

void
streams_free(Streams* self)
{
	if (self->stream)
	{
		stream_free(self->stream);
		am_free(self->stream);
		self->stream = NULL;
	}
	iov_free(&self->iov);
}

static void
streams_add(Streams* self, Stream* stream, StreamPos* pos, Part* part, Timeline* timeline)
{
	stream_init(stream, self, part, timeline);
	if (pos)
		stream->pos = *pos;
	event_attach(&stream->on_complete);
	event_set_parent(&stream->on_complete, &self->notify);
	event_attach(&stream->on_cancel);
	event_set_parent(&stream->on_cancel, &self->notify);
	stream->ready = true;

	list_append(&self->ready, &stream->link);
}

void
streams_create_for(Streams* self, Part* part, Timeline* timeline, Str* id)
{
	StreamPos pos;
	stream_pos_init(&pos);
	if (stream_pos_read(&pos, id) == -1)
		error("stream: invalid id");
	self->stream = am_malloc(sizeof(Stream));
	streams_add(self, self->stream, &pos, part, timeline);
}

void
streams_create(Streams* self, Part* part, Timeline* timeline)
{
	self->stream = am_malloc(sizeof(Stream));
	streams_add(self, self->stream, NULL, part, timeline);
}

hot static void
streams_main(Streams* self)
{
	auto client = self->client;
	(void)client;

#if 0
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
#endif
}

static void
streams_shutdown(Streams* self)
{
	(void)self;
#if 0
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
#endif
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
