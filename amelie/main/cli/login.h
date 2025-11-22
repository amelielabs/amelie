#pragma once

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

typedef struct Login Login;

struct Login
{
	Remote remote;
	List   link;
};

static inline Login*
login_allocate(void)
{
	Login* self;
	self = am_malloc(sizeof(*self));
	remote_init(&self->remote);
	list_init(&self->link);
	return self;
}

static inline void
login_free(Login* self)
{
	remote_free(&self->remote);
	am_free(self);
}

static inline void
login_set_remote(Login* self, Remote* remote)
{
	remote_copy(&self->remote, remote);
}

static inline Login*
login_copy(Login* self)
{
	auto copy = login_allocate();
	login_set_remote(copy, &self->remote);
	return copy;
}

static inline Login*
login_read(uint8_t** pos)
{
	auto self = login_allocate();
	errdefer(login_free, self);
	Decode obj[REMOTE_MAX + 1];
	for (int id = 0; id < REMOTE_MAX; id++)
	{
		obj[id].flags = DECODE_STRING;
		obj[id].key   = remote_nameof(id);
		obj[id].value = remote_get(&self->remote, id);
	}
	memset(&obj[REMOTE_MAX], 0, sizeof(Decode));
	decode_obj(obj, "login", pos);
	return self;
}

static inline void
login_write(Login* self, Buf* buf)
{
	encode_obj(buf);
	for (int id = 0; id < REMOTE_MAX; id++)
	{
		encode_cstr(buf, remote_nameof(id));
		encode_string(buf, remote_get(&self->remote, id));
	}
	encode_obj_end(buf);
}
