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
	User* self = am_malloc(sizeof(*self));
	list_init(&self->link);
	self->config = user_config_copy(config);
	return self;
}
