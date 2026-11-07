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

typedef struct Apis Apis;

struct Apis
{
	List list;
	int  list_count;
};

static inline void
apis_init(Apis* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
apis_free(Apis* self)
{
	list_foreach_safe(&self->list)
	{
		auto api = list_at(Api, link);
		api_free(api);
	}
}

static inline Api*
apis_first(Apis* self)
{
	assert(self->list_count >= 1);
	return container_of(list_first(&self->list), Api, link);
}

static inline void
apis_add(Apis* self, Api* api)
{
	list_append(&self->list, &api->link);
	self->list_count++;
}

static inline void
apis_remove(Apis* self, Api* api)
{
	list_unlink(&api->link);
	self->list_count--;
}

static inline void
apis_copy(Apis* self, Apis* from)
{
	list_foreach(&from->list)
	{
		auto api = list_at(Api, link);
		auto api_ref = api_copy(api);
		apis_add(self, api_ref);
	}
}

hot static inline Api*
apis_find(Apis* self, Str* uri)
{
	list_foreach(&self->list)
	{
		auto api = list_at(Api, link);
		if (str_compare(&api->uri, uri))
			return api;
	}
	return NULL;
}

static inline void
apis_read(Apis* self, uint8_t** pos)
{
	// [{}, ...]
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		auto api = api_read(pos);
		apis_add(self, api);
	}
}

static inline void
apis_write(Apis* self, Buf* buf, int flags)
{
	// [{}, ...]
	unused(flags);
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto api = list_at(Api, link);
		api_write(api, buf, flags);
	}
	encode_array_end(buf);
}
