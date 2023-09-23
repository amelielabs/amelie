#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestSet RequestSet;

struct RequestSet
{
	List  list;
	int   list_count;
	int   sent;
	int   complete;
	Event parent;
};

static inline void
request_set_init(RequestSet* self)
{
	self->list_count = 0;
	self->sent       = 0;
	self->complete   = 0;
	list_init(&self->list);
	event_init(&self->parent);
}

static inline void
request_set_free(RequestSet* self)
{
	assert(self->list_count == 0);
}

static inline void
request_set_reset(RequestSet* self, RequestCache* cache)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Request, link);
		request_cache_push(cache, req);
	}
	self->sent       = 0;
	self->complete   = 0;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
request_set_add(RequestSet* self, Request* req)
{
	event_set_parent(&req->src.on_write.event, &self->parent);

	self->list_count++;
	list_append(&self->list, &req->link);
}

static inline void
request_set_execute(RequestSet* self)
{
	list_foreach(&self->list)
	{
		auto req = list_at(Request, link);
		auto buf = msg_create(RPC_REQUEST);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);

		channel_write(req->dst, buf);
		self->sent++;
	}
}

static inline void
request_drain(Request* self)
{
	while (! self->complete)
	{
		auto buf = channel_read(&self->src, 0);
		if (buf == NULL)
			break;
		auto msg = msg_of(buf);
		switch (msg->id) {
		case MSG_OBJECT:
			break;
		case MSG_OK:
			self->complete = true;
			break;
		case MSG_ERROR:
			self->complete = true;
			self->error    = buf;
			continue;
		default:
			break;
		}
		buf_free(buf);
	}
}

static inline void
request_set_wait(RequestSet* self)
{
	coroutine_cancel_pause(mn_self());

	while (self->complete != self->sent)
	{
		event_wait(&self->parent, -1);

		list_foreach(&self->list)
		{
			auto req = list_at(Request, link);
			if (req->complete)
				continue;
			request_drain(req);
			if (req->complete)
				self->complete++;
		}
	}

	coroutine_cancel_resume(mn_self());
}
