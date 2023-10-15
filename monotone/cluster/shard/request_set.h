#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestSet RequestSet;

struct RequestSet
{
	Request**     set;
	int           set_size;
	int           sent;
	int           complete;
	int           error;
	Event         parent;
	RequestCache* cache;
};

static inline void
request_set_init(RequestSet* self, RequestCache* cache)
{
	self->set      = NULL;
	self->set_size = 0;
	self->sent     = 0;
	self->complete = 0;
	self->error    = 0;
	self->cache    = cache;
	event_init(&self->parent);
}

static inline void
request_set_free(RequestSet* self)
{
	if (self->set)
	{
		for (int i = 0; i < self->set_size; i++)
		{
			auto req = self->set[i];
			if (req)
				request_cache_push(self->cache, req);
		}
		mn_free(self->set);
	}
}

static inline bool
request_set_created(RequestSet* self)
{
	return self->set != NULL;
}

static inline void
request_set_create(RequestSet* self, int size)
{
	self->set = mn_malloc(sizeof(Request) * size);
	self->set_size = size;
	memset(self->set, 0, sizeof(Request) * size);
}

static inline void
request_set_reset(RequestSet* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (req)
			request_cache_push(self->cache, req);
	}
	self->sent     = 0;
	self->complete = 0;
	self->error    = 0;
}

static inline Request*
request_set_add(RequestSet* self, int order, Channel* dest)
{
	auto req = self->set[order];
	if (! req)
	{
		req = request_create(self->cache);
		req->dst = dest;
		event_set_parent(&req->src.on_write.event, &self->parent);
		self->set[order] = req;
	}
	return req;
}

hot static inline void
request_set_execute(RequestSet* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (! req)
			continue;

		auto buf = msg_create(RPC_REQUEST);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);

		channel_write(req->dst, buf);
		self->sent++;
	}
}

hot static inline void
request_drain(Request* self, Portal* portal)
{
	while (! self->complete)
	{
		auto buf = channel_read(&self->src, 0);
		if (buf == NULL)
			break;
		auto msg = msg_of(buf);
		switch (msg->id) {
		case MSG_OBJECT:
			portal_write(portal, buf);
			continue;
		case MSG_OK:
			self->complete = true;
			break;
		case MSG_ERROR:
			if (self->error)
				break;
			self->error = buf;
			continue;
		default:
			break;
		}
		buf_free(buf);
	}
}

hot static inline void
request_set_wait(RequestSet* self, Portal* portal)
{
	coroutine_cancel_pause(mn_self());

	while (self->complete != self->sent)
	{
		event_wait(&self->parent, -1);

		for (int i = 0; i < self->set_size; i++)
		{
			auto req = self->set[i];
			if (!req || req->complete)
				continue;
			request_drain(req, portal);
			if (req->complete)
			{
				event_set_parent(&req->src.on_write.event, NULL);
				self->complete++;
			}
			if (req->error)
				self->error++;
		}
	}

	coroutine_cancel_resume(mn_self());
}

hot static inline void
request_set_commit_prepare(RequestSet* self, LogSet* set)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (! req)
			continue;
		if (req->trx.log.count_persistent == 0)
			continue;
		log_set_add(set, &req->trx.log);
	}
}

hot static inline void
request_set_commit(RequestSet* self, uint64_t lsn)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (! req)
			continue;

		transaction_set_lsn(&req->trx, lsn);

		auto buf = msg_create(RPC_REQUEST_COMMIT);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);
		channel_write(req->dst, buf);
		self->set[i] = NULL;
	}
}

hot static inline void
request_set_abort(RequestSet* self)
{
	Buf* error_msg = NULL;
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (!req)
			continue;

		// reuse error message
		bool error_has = false;
		if (req->error)
		{
			error_has = true;
			if (! error_msg)
				error_msg = req->error;
			else
				buf_free(req->error);
			req->error = NULL;
		}

		self->set[i] = NULL;
		if (error_has)
		{
			// put request back to the cache
			request_cache_push(req->cache, req);
			continue;
		}

		auto buf = msg_create(RPC_REQUEST_ABORT);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);
		channel_write(req->dst, buf);
	}

	if (! error_msg)
		error("transaction aborted");

	buf_pin(error_msg);
	guard(guard, buf_free, error_msg);

	// rethrow error message
	uint8_t* pos = msg_of(error_msg)->data;
	int count;
	data_read_map(&pos, &count);
	// code
	data_skip(&pos);
	data_skip(&pos);
	// msg
	data_skip(&pos);
	Str text;
	data_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}
