#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct UserMgr UserMgr;

struct UserMgr
{
	int  list_count;
	List list;
};

static inline void
user_mgr_init(UserMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
user_mgr_free(UserMgr* self)
{
	list_foreach_safe(&self->list) {
		auto user = list_at(User, link);
		user_free(user);
	}
}

static inline Buf*
user_mgr_list(UserMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// map
	encode_map(buf, self->list_count);
	list_foreach(&self->list)
	{
		// name: {}
		auto user = list_at(User, link);
		encode_string(buf, &user->config->name);
		user_config_write_safe(user->config, buf);
	}
	msg_end(buf);
	return buf;
}

static inline void
user_mgr_save(UserMgr* self)
{
	// create dump
	auto dump = buf_create(0);
	encode_map(dump, self->list_count);
	list_foreach(&self->list)
	{
		auto user = list_at(User, link);
		encode_string(dump, &user->config->name);
		user_config_write(user->config, dump);
	}

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
	data_read_map(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		// name
		data_skip(&pos);

		// value
		auto config = user_config_read(&pos);
		guard(config_guard, user_config_free, config);

		auto user = user_allocate(config);
		list_append(&self->list, &user->link);
		self->list_count++;
	}
}

static inline User*
user_mgr_find(UserMgr* self, Str* name)
{
	list_foreach(&self->list) {
		auto user = list_at(User, link);
		if (str_compare(&user->config->name, name))
			return user;
	}
	return NULL;
}

static inline void
user_mgr_create(UserMgr* self, UserConfig* config, bool if_not_exists)
{
	auto user = user_mgr_find(self, &config->name);
	if (user)
	{
		if (! if_not_exists)
			error("user '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}
	user = user_allocate(config);
	list_append(&self->list, &user->link);
	self->list_count++;

	user_mgr_save(self);
}

static inline void
user_mgr_drop(UserMgr* self, Str* name, bool if_exists)
{
	auto user = user_mgr_find(self, name);
	if (! user)
	{
		if (! if_exists)
			error("user '%.*s': not exists", str_size(name), str_of(name));
		return;
	}
	list_unlink(&user->link);
	self->list_count--;
	user_free(user);
	user_mgr_save(self);
}

static inline void
user_mgr_alter(UserMgr* self, UserConfig* config)
{
	auto user = user_mgr_find(self, &config->name);
	if (! user)
	{
		error("user '%.*s': not exists", str_size(&config->name),
		      str_of(&config->name));
		return;
	}
	user_config_set_secret(user->config, &config->secret);
	user_mgr_save(self);
}
