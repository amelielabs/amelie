
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>

void
user_mgr_init(UserMgr* self)
{
	user_cache_init(&self->cache);
}

void
user_mgr_free(UserMgr* self)
{
	user_cache_free(&self->cache);
}

static inline void
user_mgr_save(UserMgr* self)
{
	// create dump
	auto buf = user_cache_dump(&self->cache);
	guard_buf(buf);

	// update and save state
	var_data_set_buf(&config()->users, buf);
	control_save_config();
}

void
user_mgr_open(UserMgr* self)
{
	auto users = &config()->users;
	if (! var_data_is_set(users))
		return;
	auto pos = var_data_of(users);
	if (data_is_null(pos))
		return;

	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		// value
		auto config = user_config_read(&pos);
		guard(user_config_free, config);

		auto user = user_allocate(config);
		user_cache_add(&self->cache, user);
	}
}

void
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

void
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

void
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

Buf*
user_mgr_list(UserMgr* self)
{
	return user_cache_list(&self->cache);
}
