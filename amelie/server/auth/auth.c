
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>

void
auth_init(Auth* self)
{
	auth_state_init(&self->state);
	auth_cache_init(&self->cache);
	user_cache_init(&self->user_cache);
}

void
auth_free(Auth* self)
{
	auth_state_free(&self->state);
	auth_cache_free(&self->cache);
	user_cache_free(&self->user_cache);
}

void
auth_sync(Auth* self, UserCache* cache)
{
	auth_cache_reset(&self->cache);
	user_cache_sync(&self->user_cache, cache);
}

hot static inline User*
auth_run(Auth* self, Str* token)
{
	auto state = &self->state;
	auth_state_reset(state);

	// parse authentication token
	auth_state_read(state, token);

	// check auth cache, if the digest is already authenticated
	auto user = auth_cache_find(&self->cache, &state->digest);
	if (user)
		return user;

	// process with authentication

	// validate header
	auth_state_read_header(state);

	// validate payload, read role
	Str role;
	str_init(&role);
	auth_state_read_payload(state, &role);

	// find user
	user = user_cache_find(&self->user_cache, &role);
	if (! user)
		error("auth: user %.*s not found", str_size(&user->config->name),
		      str_of(&user->config->name));

	// validate digest using user secret
	auth_state_validate(state, &user->config->secret);

	// add user and token digest to the cache
	auth_cache_add(&self->cache, user, &state->digest);
	return user;
}

hot User*
auth(Auth* self, Str* token)
{
	User* user = NULL;
	Exception e;
	if (enter(&e))
		user = auth_run(self, token);
	if (leave(&e))
	{ }
	return user;
}
