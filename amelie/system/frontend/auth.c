
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
#include <amelie_repl>
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
auth_jwt(Auth* self, Endpoint* endpoint)
{
	auto jwt = &self->jwt;
	jwt_decode_reset(jwt);
	auth_cache_prepare(&self->cache);

	// parse jwt token
	Str token = *opt_string_of(&endpoint->token);
	str_advance(&token, 7);
	jwt_decode(jwt, &token);

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

		error("auth: user '{str}' token is invalid",
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
auth_basic(Auth* self, Endpoint* endpoint)
{
	unused(self);

	// parse basic token
	Str token = *opt_string_of(&endpoint->token);
	str_advance(&token, 6);

	Str name;
	Str password;
	auto buf = basic_decode(&token, &name, &password);
	defer_buf(buf);

	// find user
	auto user = catalog_find_user(&share()->db->catalog, &name, false);
	if (! user)
		error("auth: user '{str}' not found", &name);
	return user;
}

hot User*
auth(Auth* self, Endpoint* endpoint)
{
	// no token
	auto token = opt_string_of(&endpoint->token);
	if (str_empty(token))
	{
		// allow only for trusted connections (localhost)
		if (! opt_int_of(&endpoint->trusted))
			error("auth: authentication token is missing");

		Str name;
		str_set(&name, "amelie", 6);
		return catalog_find_user(&share()->db->catalog, &name, true);
	}

	// basic (only local)
	if (str_is_prefix_case(token, "Basic ", 6))
	{
		// allow only for trusted connections (localhost)
		if (! opt_int_of(&endpoint->trusted))
			error("auth: basic token allowed only for trusted connections");

		return auth_basic(self, endpoint);
	}

	// jwt
	if (str_is_prefix_case(token, "Bearer ", 7))
		return auth_jwt(self, endpoint);

	error("auth: invalid token");
	return NULL;
}
