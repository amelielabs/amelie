#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	so_free(user);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self;
	self = so_malloc(sizeof(*self));
	self->config = NULL;
	list_init(&self->link);
	guard(user_free, self);
	self->config = user_config_copy(config);
	return unguard();
}
