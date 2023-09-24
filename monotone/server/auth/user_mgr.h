#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct UserMgr UserMgr;

struct UserMgr
{
	UserCache cache;
};

static inline void
user_mgr_init(UserMgr* self)
{
	user_cache_init(&self->cache);
}

static inline void
user_mgr_free(UserMgr* self)
{
	user_cache_free(&self->cache);
}

static inline Buf*
user_mgr_list(UserMgr* self)
{
	return user_cache_list(&self->cache);
}

static inline void
user_mgr_save(UserMgr* self)
{
	// create dump
	auto dump = user_cache_dump(&self->cache);

	// update and save state
	var_data_set_buf(&config()->users, dump);
	control_save_config();
	buf_free(dump);
}

static inline void
user_mgr_open(UserMgr* self)
{
	auto users = &config()->users;
	if (! var_data_is_set(users))
		return;
	auto pos = var_data_of(users);
	if (data_is_null(pos))
		return;

	int count;
	data_read_array(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		// value
		auto config = user_config_read(&pos);
		guard(config_guard, user_config_free, config);

		auto user = user_allocate(config);
		user_cache_add(&self->cache, user);
	}
}

static inline void
user_mgr_create(UserMgr* self, UserConfig* config, bool if_not_exists)
{
	auto user = user_cache_find(&self->cache, &config->name);
	if (user)
	{
		if (! if_not_exists)
			error("user '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}
	user = user_allocate(config);
	user_cache_add(&self->cache, user);
	user_mgr_save(self);
}

static inline void
user_mgr_drop(UserMgr* self, Str* name, bool if_exists)
{
	auto user = user_cache_find(&self->cache, name);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name), str_of(name));
		return;
	}
	user_cache_delete(&self->cache, user);
	user_mgr_save(self);
}

static inline void
user_mgr_alter(UserMgr* self, UserConfig* config)
{
	auto user = user_cache_find(&self->cache, &config->name);
	if (! user)
	{
		error("user '%.*s': not exists", str_size(&config->name),
		      str_of(&config->name));
		return;
	}
	str_free(&user->config->secret);
	user_config_set_secret(user->config, &config->secret);
	user_mgr_save(self);
}
