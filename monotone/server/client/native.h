#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Native Native;

struct Native
{
	Channel  src;
	Channel  core;
	bool     connected;
	uint64_t coroutine_id;
	bool     relay;
	Str      uri;
	void*    arg;
	List     link;
};

static inline void
native_init(Native* self)
{
	self->connected    = false;
	self->coroutine_id = UINT64_MAX;
	self->relay        = false;
	self->arg          = NULL;
	str_init(&self->uri);
	channel_init(&self->src);
	channel_init(&self->core);
	list_init(&self->link);
}

static inline void
native_free(Native* self, BufCache* buf_cache)
{
	channel_free(&self->src, buf_cache);
	channel_free(&self->core, buf_cache);
	str_free(&self->uri);
}

static inline int
native_set_uri(Native* self, const char* spec)
{
	return str_strndup_nothrow(&self->uri, spec, strlen(spec));
}

static inline void
native_set_connected(Native* self, bool value)
{
	self->connected = value;
}

static inline void
native_set_coroutine(Native* self)
{
	self->coroutine_id = mn_self()->id;
}

static inline void
native_set_coroutine_name(Native* self)
{
	if (self->relay)
		coroutine_set_name(mn_self(), "native (relay)");
	else
		coroutine_set_name(mn_self(), "native");
}

static inline void
native_on_connect(Native* self)
{
	auto buf = msg_create(MSG_CONNECT);
	channel_write(&self->src, buf);
}

static inline void
native_on_disconnect(Native* self)
{
	auto buf = msg_create(MSG_DISCONNECT);
	channel_write(&self->src, buf);
}

static inline Buf*
native_connect(Native* self, BufCache* buf_cache)
{
	auto buf = msg_create_nothrow(buf_cache, MSG_NATIVE, sizeof(void**));
	if (unlikely(buf == NULL))
		return NULL;
	buf_append(buf, &self, sizeof(void**));
	msg_end(buf);
	return buf;
}

static inline int
native_close(Native* self, BufCache* buf_cache)
{
	if (! self->connected)
		return 0;

	// send DISCONNECT
	auto buf = msg_create_nothrow(buf_cache, MSG_DISCONNECT, 0);
	if (unlikely(buf == NULL))
		return -1;
	channel_write(&self->core, buf);

	// wait DISCONNECT event
	for (;;)
	{
		buf = channel_read(&self->src, -1);
		if (buf == NULL)
			continue;
		int id = msg_of(buf)->id;
		buf_cache_push(buf_cache, buf);
		if (id == MSG_DISCONNECT)
			break;
	}

	native_set_connected(self, false);
	return 0;
}

static inline int
native_command(Native*        self,
               BufCache*      buf_cache,
               Str*           text,
               int            argc,
               CommandArgPtr* argv)
{
	auto buf = command_create(buf_cache, text, argc, argv);
	if (unlikely(buf == NULL))
		return -1;
	channel_write(&self->core, buf);
	return 0;
}
