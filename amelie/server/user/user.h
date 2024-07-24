#pragma once

//
// amelie.
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
	am_free(user);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self;
	self = am_malloc(sizeof(*self));
	self->config = NULL;
	list_init(&self->link);
	guard(user_free, self);
	self->config = user_config_copy(config);
	return unguard();
}
