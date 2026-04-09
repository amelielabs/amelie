
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_pub.h>
#include <amelie_sub.h>

void
sub_mgr_init(SubMgr* self, Pub* pub)
{
	self->pub        = pub;
	self->list_count = 0;
	list_init(&self->list);
}

void
sub_mgr_free(SubMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto sub = list_at(Sub, link);
		pub_detach(self->pub, &sub->slot);
		// todo: unref?
		sub_free(sub);
	}
}

static inline void
sub_mgr_save(SubMgr* self)
{
	// create dump
	auto buf = buf_create();
	defer_buf(buf);

	encode_array(buf);
	list_foreach(&self->list)
	{
		auto sub = list_at(Sub, link);
		sub_config_write(sub->config, buf, 0);
	}
	encode_array_end(buf);

	// update and save state
	opt_json_set_buf(&state()->subs, buf);
}

void
sub_mgr_open(SubMgr* self)
{
	auto subs = &state()->subs;
	if (opt_json_empty(subs))
		return;
	auto pos = opt_json_of(subs);
	if (json_is_null(pos))
		return;

	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		auto config = sub_config_read(&pos);
		defer(sub_config_free, config);

		auto sub = sub_allocate(config);
		list_append(&self->list, &sub->link);
		self->list_count++;

		// attach slot
		pub_attach(self->pub, &sub->slot);
	}
}

void
sub_mgr_create(SubMgr* self, SubConfig* config, bool if_not_exists)
{
	auto sub = sub_mgr_find(self, &config->user, &config->name, false);
	if (sub)
	{
		if (! if_not_exists)
			error("subscription '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}
	sub = sub_allocate(config);
	list_append(&self->list, &sub->link);
	self->list_count++;

	// todo: set lsn and prepare slot

	// attach slot
	pub_attach(self->pub, &sub->slot);

	sub_mgr_save(self);
}

void
sub_mgr_drop(SubMgr* self, Str* user, Str* name, bool if_exists)
{
	auto sub = sub_mgr_find(self, user, name, false);
	if (! sub)
	{
		if (! if_exists)
			error("subscription '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	list_unlink(&sub->link);
	self->list_count--;

	// detach slot
	pub_detach(self->pub, &sub->slot);

	sub_mgr_save(self);

	// todo: unref?
		// sub_free(sub);
}

Buf*
sub_mgr_list(SubMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show subscription
		auto sub = sub_mgr_find(self, user, name, false);
		if (sub)
			sub_config_write(sub->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show subscriptions
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto sub = list_at(Sub, link);
		if (user && !str_compare_case(&sub->config->user, user))
			continue;
		sub_config_write(sub->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}

Sub*
sub_mgr_find(SubMgr* self, Str* user, Str* name,
             bool    error_if_not_exists)
{
	list_foreach(&self->list)
	{
		auto sub = list_at(Sub, link);
		if (str_compare_case(&sub->config->user, user) &&
		    str_compare_case(&sub->config->name, name))
			return sub;
	}
	if (error_if_not_exists)
		error("subscription '%.*s': not exists", str_size(name),
		      str_of(name));
	return NULL;
}
