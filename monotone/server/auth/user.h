#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct User User;

struct User
{
	UserConfig* config;
	List        link;
};

static inline void
user_free(User* user)
{
	if (user->config)
		user_config_free(user->config);
	mn_free(user);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self;
	self = mn_malloc(sizeof(*self));
	self->config = NULL;
	list_init(&self->link);
	guard(self_guard, user_free, self);
	self->config = user_config_copy(config);
	return unguard(&self_guard);
}
