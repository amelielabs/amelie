
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>

void
auth_init(Auth* self)
{
	jwt_decode_init(&self->jwt);
	auth_cache_init(&self->cache);
}

void
auth_free(Auth* self)
{
	jwt_decode_free(&self->jwt);
	auth_cache_free(&self->cache);
}

void
auth_reset(Auth* self)
{
	auth_cache_reset(&self->cache);
}

hot static inline User*
auth_run(Auth* self, Str* user_id, Str* token)
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

		error("auth: user '{str}' token has expired",
		      &user->config->name);
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
		error("auth: user '{str}' has invalid iat/exp fields", &sub);

	// ensure token has not expired
	if (now >= exp)
		error("auth: user '{str}' token has expired", &sub);

	// ensure user_id matches sub
	if (! str_compare(&sub, user_id))
		error("auth: user '{str}' does not match sub field", &sub);

	// find user
	auto user = catalog_find_user(&share()->db->catalog, &sub, false);
	if (! user)
		error("auth: user '{str}' not found", &sub);

	// check issued time against user revoked_at
	if (iat <= user->revoked_at)
		error("auth: user '{str}' token has been revoked", &sub);

	// validate digest using user secret
	auto secret = opt_string_of(&state()->secret);
	if (! jwt_decode_validate(jwt, secret))
		error("auth: user '{str}' token is invalid", &sub);

	// add user and token digest to the cache
	auth_cache_add(&self->cache, user, &jwt->digest, exp);
	return user;
}

hot static inline User*
auth_main(Auth* self, Str* user_id, Str* token, bool token_required)
{
	if (unlikely(str_empty(user_id)))
		error("auth: user id is missing");

	User* user;
	if (str_empty(token))
	{
		if (token_required)
			error("auth: authentication token is missing");

		// trusted by the server listen configuration
		user = catalog_find_user(&share()->db->catalog, user_id, true);
	} else
	{
		user = auth_run(self, user_id, token);
	}
	return user;
}

hot User*
auth(Auth* self, Str* user_id, Str* token, bool token_required)
{
	User* user = NULL;
	auto on_error = error_catch
	(
		user = auth_main(self, user_id, token, token_required)
	);
	if (on_error)
	{
		am_self()->error.code = ERROR_AUTH;
		rethrow();
	}
	return user;
}
