
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
feeds_init(Feeds* self, Task* task, Client* client)
{
	self->feeds       = NULL;
	self->feeds_count = 0;
	self->task        = task;
	self->client      = client;
	list_init(&self->ready);
	event_init(&self->notify);
	iov_init(&self->iov);
}

void
feeds_free(Feeds* self)
{
	if (! self->feeds)
		return;

	for (auto i = 0; i < self->feeds_count; i++)
		feed_free(&self->feeds[i]);

	iov_free(&self->iov);
	am_free(self->feeds);
	self->feeds = NULL;
}

void
feeds_create(Feeds* self, Parts* parts, Str* key)
{
	self->feeds_count = parts->list_count;
	self->feeds = am_malloc(sizeof(Feed) * self->feeds_count);

	// prepare feeds per partitions and set as ready
	auto at = 0;
	list_foreach(&parts->list)
	{
		auto part = list_at(Part, link);
		auto feed = &self->feeds[at];
		feed_init(feed, self, part->track.backend, part);
		feed->key   = key;
		feed->ready = true;
		list_append(&self->ready, &feed->link);
		at++;
	}
}

hot static void
feeds_main(Feeds* self)
{
	auto client = self->client;
	auto iov = &self->iov;
	for (;;)
	{
		// send to all ready feeds
		while (! list_empty(&self->ready))
		{
			auto feed = container_of(list_pop(&self->ready), Feed, link);
			feed->ready = false;
			feed_request(feed);
		}
		list_init(&self->ready);

		// wait for results
		//
		// todo: check client disconnect event
		event_wait(&self->notify, 0);

		// batch send
		iov_reset(iov);
		list_foreach(&self->ready)
		{
			auto feed = container_of(list_pop(&self->ready), Feed, link);
			iov_add_buf(iov, &feed->data);
		}
		if (iov_empty(iov))
			continue;

		tcp_write(&client->tcp, iov_pointer(iov), iov->iov_count);
	}
}

static void
feeds_shutdown(Feeds* self)
{
	// send FEED_CANCEL cancel active feeds
	auto wait = false;
	for (auto i = 0; i < self->feeds_count; i++)
	{
		auto feed = &self->feeds[i];
		if (feed->ready)
		{
			feed->cancel = true;
			continue;
		}
		feed_cancel(feed);
		wait = true;
	}
	if (! wait)
		return;

	cancel_pause();

	// wait for all feeds to be canceled
	for (;;)
	{
		wait = false;
		for (auto i = 0; i < self->feeds_count; i++)
		{
			if (! self->feeds[i].cancel)
			{
				wait = true;
				break;
			}
		}
		if (! wait)
			break;

		event_wait(&self->notify, 0);
	}

	cancel_resume();
}

void
feeds_run(Feeds* self)
{
	// relay feeds data to the client
	error_catch( feeds_main(self) );

	// cancel and ensure everyone finished
	feeds_shutdown(self);
}

#if 0
hot static inline bool
link_wait(Link* self, StreamCursor* cursor)
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

	// prepare stream subscription
	StreamSub sub;
	stream_sub_init(&sub, &event_sub, cursor->id);
	stream_subscribe(cursor->stream, &sub);

	// wait
	auto on_error = error_catch
	(
		poll_read_start(&self->client->tcp.fd, &event_client);
		event_wait(&event, -1);
	);
	poll_read_stop(&self->client->tcp.fd);
	stream_unsubscribe(cursor->stream, &sub);

	if (unlikely(on_error))
		rethrow();

	// client disconnect on stream shutdown
	return event_client.signal || sub.shutdown;
}
#endif
