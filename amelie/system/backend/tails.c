
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

	iov_free(&self->iov);
	am_free(self->tails);
	self->tails = NULL;
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
		tail_init(tail, self, self->task, part->track.backend, part);
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

	auto iov = &self->iov;
	for (;;)
	{
		// send to all ready tails
		while (! list_empty(&self->ready))
		{
			auto tail = container_of(list_pop(&self->ready), Tail, link);
			tail->ready = false;
			task_send(tail->part_task, &tail->msg);
		}
		list_init(&self->ready);

		// wait for results
		//
		// todo: check client disconnect event
		event_wait(&self->notify, -1);

		// eof
		if (unlikely(eof.signal))
			break;

		// batch send
		iov_reset(iov);
		list_foreach(&self->ready)
		{
			auto tail = container_of(list_pop(&self->ready), Tail, link);
			iov_add_buf(iov, &tail->data);
		}
		if (iov_empty(iov))
			continue;

		tcp_write(&client->tcp, iov_pointer(iov), iov->iov_count);
	}
}

static void
tails_shutdown(Tails* self)
{
	// send TAIL_CANCEL cancel active tails
	auto wait = false;
	for (auto i = 0; i < self->tails_count; i++)
	{
		auto tail = &self->tails[i];
		if (tail->ready)
		{
			tail->cancel = true;
			continue;
		}
		task_send(tail->part_task, &tail->msg_cancel);
		wait = true;
	}
	if (! wait)
		return;

	cancel_pause();

	// wait for all tails to be canceled
	for (;;)
	{
		wait = false;
		for (auto i = 0; i < self->tails_count; i++)
		{
			if (! self->tails[i].cancel)
			{
				wait = true;
				break;
			}
		}
		if (! wait)
			break;

		event_wait(&self->notify, -1);
	}

	cancel_resume();
}

void
tails_run(Tails* self)
{
	// relay tails data to the client
	error_catch( tails_main(self) );

	// cancel and ensure everyone finished
	tails_shutdown(self);
}

#if 0
hot static inline bool
link_wait(Link* self, TailCursor* cursor)
{
	// parent
	Event event;
	event_init(&event);
	event_attach(&event);

	// prepare client event
	Event event_client;
	event_init(&event_client);
	event_set_parent(&event_client, &event);
	event_attach(&event_client);

	// prepare sub event
	Event event_sub;
	event_init(&event_sub);
	event_set_parent(&event_sub, &event);
	event_attach(&event_sub);

	// prepare tail subscription
	TailSub sub;
	tail_sub_init(&sub, &event_sub, cursor->id);
	tail_subscribe(cursor->tail, &sub);

	// wait
	auto on_error = error_catch
	(
		poll_read_start(&self->client->tcp.fd, &event_client);
		event_wait(&event, -1);
	);
	poll_read_stop(&self->client->tcp.fd);
	tail_unsubscribe(cursor->tail, &sub);

	if (unlikely(on_error))
		rethrow();

	// client disconnect on tail shutdown
	return event_client.signal || sub.shutdown;
}
#endif
