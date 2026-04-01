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

typedef struct User User;

struct User
{
	Rel         rel;
	int64_t     revoked_at;
	UserConfig* config;
};

static inline void
user_free(User* self, bool drop)
{
	unused(drop);
	if (self->config)
		user_config_free(self->config);
	am_free(self);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self = am_malloc(sizeof(User));
	self->revoked_at = 0;
	self->config     = user_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_USER);
	rel_set_user(rel, &self->config->parent);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_free_function(rel, (RelFree)user_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline void
user_sync(User* self)
{
	if (str_empty(&self->config->revoked_at))
	{
		self->revoked_at = 0;
		return;
	}
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_set(&ts, &self->config->revoked_at);
	auto time = timestamp_get_unixtime(&ts, runtime()->timezone);
	self->revoked_at = time / 1000 / 1000;
}

static inline User*
user_of(Rel* self)
{
	return (User*)self;
}

static inline void
user_permission_error(User* self)
{
	error("user '%.*s': permission denied", str_size(&self->config->name),
	      str_of(&self->config->name));
}
