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
	UserConfig* config;
	List        link;
};

static inline User*
user_allocate(UserConfig* config)
{
	User* self = am_malloc(sizeof(*self));
	list_init(&self->link);
	self->config = user_config_copy(config);
	return self;
}

static inline void
user_free(User* user)
{
	if (user->config)
		user_config_free(user->config);
	am_free(user);
}
