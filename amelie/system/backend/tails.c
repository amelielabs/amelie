
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

void
tails_init(Tails* self, Task* task, Client* client)
{
	self->tails       = NULL;
	self->tails_count = 0;
	self->task        = task;
	self->client      = client;
	list_init(&self->ready);
	event_init(&self->notify);
	iov_init(&self->iov);
}

void
tails_free(Tails* self)
{
	if (! self->tails)
		return;

	for (auto i = 0; i < self->tails_count; i++)
		tail_free(&self->tails[i]);
	am_free(self->tails);
	self->tails = NULL;

	iov_free(&self->iov);
}

void
tails_create(Tails* self, Parts* parts, Str* key)
{
	self->tails_count = parts->list_count;
	self->tails = am_malloc(sizeof(Tail) * self->tails_count);

	// todo: prepare key
	(void)key;

	// prepare tails per partitions and set as ready
	auto at = 0;
	list_foreach(&parts->list)
	{
		auto part = list_at(Part, link);
		auto tail = &self->tails[at];
		tail_init(tail, self, part);

		event_attach(&tail->on_complete);
		event_set_parent(&tail->on_complete, &self->notify);

		event_attach(&tail->on_cancel);
		event_set_parent(&tail->on_cancel, &self->notify);

		tail->ready = true;
		list_append(&self->ready, &tail->link);
		at++;
	}
}

hot static void
tails_main(Tails* self)
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
			auto tail = container_of(list_pop(&self->ready), Tail, link);
			tail->ready = false;
			list_init(&tail->link);
			buf_reset(&tail->data);
			task_send(tail->part_task, &tail->msg);
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
		for (auto i = 0; i < self->tails_count; i++)
		{
			auto tail = &self->tails[i];
			if (! tail->on_complete.signal)
				continue;

			tail->on_complete.signal = false;
			if (tail->shutdown)
				shutdown = true;

			if (! buf_empty(&tail->data))
				iov_add_buf(iov, &tail->data);

			tail->ready = true;
			list_append(&self->ready, &tail->link);
		}
		if (unlikely(shutdown))
			break;

		// batch send
		if (! iov_empty(iov))
			tcp_write(&client->tcp, iov_pointer(iov), iov->iov_count);
	}
}

static void
tails_shutdown(Tails* self)
{
	auto pending = false;
	for (auto i = 0; i < self->tails_count; i++)
	{
		auto tail = &self->tails[i];
		if (tail->ready)
			continue;
		task_send(tail->part_task, &tail->msg_cancel);
		pending = true;
	}
	if (! pending)
		return;

	// wait for completion
	for (auto i = 0; i < self->tails_count; i++)
	{
		auto tail = &self->tails[i];
		if (tail->ready)
			continue;
		event_wait(&tail->on_complete, -1);
		event_wait(&tail->on_cancel, -1);
		tail->ready = true;
		list_append(&self->ready, &tail->link);
	}
}

void
tails_run(Tails* self)
{
	// relay tails data to the client
	error_catch( tails_main(self) );

	// cancel and ensure everyone finished
	cancel_pause();
	tails_shutdown(self);
	cancel_resume();
}
