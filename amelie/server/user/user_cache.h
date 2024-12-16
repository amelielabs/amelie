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

typedef struct UserCache UserCache;

struct UserCache
{
	int  list_count;
	List list;
};

static inline void
user_cache_init(UserCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
user_cache_free(UserCache* self)
{
	list_foreach_safe(&self->list) {
		auto user = list_at(User, link);
		user_free(user);
	}
}

static inline User*
user_cache_find(UserCache* self, Str* name)
{
	list_foreach(&self->list) {
		auto user = list_at(User, link);
		if (str_compare(&user->config->name, name))
			return user;
	}
	return NULL;
}

static inline void
user_cache_add(UserCache* self, User* user)
{
	list_append(&self->list, &user->link);
	self->list_count++;
}

static inline void
user_cache_delete(UserCache* self, User* user)
{
	list_unlink(&user->link);
	self->list_count--;
	assert(self->list_count >= 0);
	user_free(user);
}

static inline void
user_cache_sync(UserCache* self, UserCache* with)
{
	// remove all users that are no longer exists
	list_foreach_safe(&self->list)
	{
		auto user = list_at(User, link);
		auto user_with = user_cache_find(with, &user->config->name);

		// delete user or sync config
		if (! user_with)
			user_cache_delete(self, user);
		else
			user_config_sync(user->config, user_with->config);
	}

	// add new users
	list_foreach(&with->list)
	{
		auto user = list_at(User, link);
		if (! user_cache_find(self, &user->config->name))
		{
			auto new_user = user_allocate(user->config);
			user_cache_add(self, new_user);
		}
	}

	assert(self->list_count == with->list_count);
}

static inline Buf*
user_cache_dump(UserCache* self)
{
	auto buf = buf_create();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto user = list_at(User, link);
		user_config_write(user->config, buf, true);
	}
	encode_array_end(buf);
	return buf;
}

static inline Buf*
user_cache_list(UserCache* self, Str* name)
{
	auto buf = buf_create();
	if (name)
	{
		auto user = user_cache_find(self, name);
		if (! user)
			encode_null(buf);
		else
			user_config_write(user->config, buf, false);
	} else
	{
		encode_array(buf);
		list_foreach(&self->list)
		{
			auto user = list_at(User, link);
			user_config_write(user->config, buf, false);
		}
		encode_array_end(buf);
	}
	return buf;
}
