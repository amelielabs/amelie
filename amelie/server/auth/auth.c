
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
	jwt_decode_init(&self->jwt);
	auth_cache_init(&self->cache);
	user_cache_init(&self->user_cache);
}

void
auth_free(Auth* self)
{
	jwt_decode_free(&self->jwt);
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
	auto jwt = &self->jwt;
	jwt_decode_reset(jwt);
	auth_cache_prepare(&self->cache);

	// parse authentication token
	jwt_decode(jwt, token);

	// check auth cache, if the digest is already authenticated
	auto user = auth_cache_find(&self->cache, &jwt->digest);
	if (user)
		return user;

	// process with authentication

	// validate header, payload and read role
	Str role;
	str_init(&role);
	jwt_decode_data(jwt, &role);

	// find user
	user = user_cache_find(&self->user_cache, &role);
	if (! user)
		error("auth: user %.*s not found", str_size(&role), str_of(&role));

	// validate digest using user secret
	jwt_decode_validate(jwt, &user->config->secret);

	// add user and token digest to the cache
	auth_cache_add(&self->cache, user, &jwt->digest);
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
