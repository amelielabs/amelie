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
	self->config = user_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_USER);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)user_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline User*
user_of(Rel* self)
{
	return (User*)self;
}
