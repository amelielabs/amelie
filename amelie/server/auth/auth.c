
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
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

	int64_t now = time_ms() / 1000;

	// check auth cache, if the digest is already authenticated
	auto user_ref = auth_cache_find(&self->cache, &jwt->digest);
	if (user_ref)
	{
		// ensure cached token has not expired
		auto user = user_ref->user;
		if (likely(now < user_ref->expire))
			return user;

		// remove from the cache
		auth_cache_del(&self->cache, user_ref);

		error("auth: user '%.*s' token has expired",
		      str_size(&user->config->name),
		      str_of(&user->config->name));
	}

	// process with authentication

	// validate header, payload and read role, iat and exp
	int64_t iat;
	int64_t exp;
	Str     sub;
	str_init(&sub);
	jwt_decode_data(jwt, &sub, &iat, &exp);

	// validate issue/expire fields
	if (!iat || !exp || iat > exp)
		error("auth: user '%.*s' has invalid iat/exp fields",
		      str_size(&sub), str_of(&sub));

	// ensure token has not expired
	if (now >= exp)
		error("auth: user '%.*s' token has expired",
		      str_size(&sub), str_of(&sub));

	// find user
	auto user = user_cache_find(&self->user_cache, &sub);
	if (! user)
		error("auth: user '%.*s' not found", str_size(&sub),
		      str_of(&sub));

	// validate digest using user secret
	jwt_decode_validate(jwt, &user->config->secret);

	// add user and token digest to the cache
	auth_cache_add(&self->cache, user, &jwt->digest, exp);
	return user;
}

hot User*
auth(Auth* self, Str* token)
{
	User* user = NULL;
	error_catch(
		user = auth_run(self, token)
	);
	return user;
}
