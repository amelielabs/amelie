#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Client Client;

struct Client
{
	uint64_t   id;
	Access     access;
	AccessMode mode;
	Auth*      auth;
	Channel    src;
	Channel    core;
	bool       connected;
	uint64_t   coroutine_id;
	bool       relay;
	Str        uri;
	void*      arg;
	List       link;
};

static inline void
client_init(Client* self)
{
	self->id           = 0;
	self->access       = ACCESS_UNDEF;
	self->mode         = ACCESS_MODE_ANY;
	self->auth         = NULL;
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
client_free(Client* self, BufCache* buf_cache)
{
	channel_free(&self->src, buf_cache);
	channel_free(&self->core, buf_cache);
	str_free(&self->uri);
}

static inline void
client_set_access(Client* self, Access access)
{
	self->access = access;
}

static inline void
client_set_mode(Client* self, AccessMode mode)
{
	self->mode = mode;
}

static inline void
client_set_auth(Client* self, Auth* auth)
{
	self->auth = auth;
}

static inline int
client_set_uri(Client* self, const char* spec)
{
	return str_strndup_nothrow(&self->uri, spec, strlen(spec));
}

static inline void
client_set_connected(Client* self, bool value)
{
	self->connected = value;
}

static inline void
client_set_coroutine(Client* self)
{
	self->coroutine_id = mn_self()->id;
}

static inline void
client_set_coroutine_name(Client* self)
{
	if (self->access == ACCESS_CLIENT)
	{
		coroutine_set_name(mn_self(), "client %" PRIu64, self->id);
	} else
	{
		Str access;
		access_str(self->access, &access);
		coroutine_set_name(mn_self(), "client %" PRIu64 " (%.*s)",
		                   self->id,
		                   str_size(&access),
		                   str_of(&access));
	}
}

static inline void
client_on_connect(Client* self)
{
	auto buf = msg_create(MSG_CONNECT);
	channel_write(&self->src, buf);
}

static inline void
client_on_disconnect(Client* self)
{
	auto buf = msg_create(MSG_DISCONNECT);
	channel_write(&self->src, buf);
}

static inline Buf*
client_connect(Client* self, BufCache* buf_cache)
{
	auto buf = msg_create_nothrow(buf_cache, MSG_CLIENT, sizeof(void**));
	if (unlikely(buf == NULL))
		return NULL;
	buf_append(buf, &self, sizeof(void**));
	msg_end(buf);
	return buf;
}

static inline int
client_close(Client* self, BufCache* buf_cache)
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

	client_set_connected(self, false);
	return 0;
}

static inline int
client_request(Client*        self,
               BufCache*      buf_cache,
               Str*           text,
               int            argc,
               RequestArgPtr* argv)
{
	auto buf = request_create(buf_cache, text, argc, argv);
	if (unlikely(buf == NULL))
		return -1;
	channel_write(&self->core, buf);
	return 0;
}
