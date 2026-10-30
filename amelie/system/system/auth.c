
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
#include <amelie_compiler>
#include <amelie_compute.h>
#include <amelie_session.h>
#include <amelie_system.h>

void
auth_init(Auth* self)
{
	self->amelie = NULL;
	jwt_decode_init(&self->jwt);
}

void
auth_free(Auth* self)
{
	jwt_decode_free(&self->jwt);
}

void
auth_reset(Auth* self)
{
	self->amelie = NULL;
}

hot static inline User*
auth_jwt(Auth* self, Str* token, int64_t* expire)
{
	auto jwt = &self->jwt;
	jwt_decode_reset(jwt);

	// parse token
	Str bearer = *token;
	str_advance(&bearer, 7);
	jwt_decode(jwt, &bearer);

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
	int64_t now = time_us();
	if (now >= exp)
		error("auth: user '{str}' token has expired", &sub);

	// find user
	auto lock = lock_catalog(LOCK_SHARED);
	defer(unlock, lock);

	auto user = catalog_find_user(&share()->db->catalog, &sub, false);
	if (! user)
		error("auth: user '{str}' not found", &sub);

	// check issued time against user revoked_at
	if (iat <= user->revoked_at)
		error("auth: user '{str}' token has been revoked", &sub);

	// validate token using secret
	auto secret = opt_string_of(&state()->secret);
	if (! jwt_decode_validate(jwt, secret))
		error("auth: user '{str}' token is invalid", &sub);

	*expire = exp;
	return user;
}

hot static inline User*
auth_basic(Auth* self, Str* token)
{
	unused(self);

	// parse basic token
	Str basic = *token;
	str_advance(&basic, 6);

	Str name;
	Str password;
	auto buf = basic_decode(&basic, &name, &password);
	defer_buf(buf);

	// find user
	auto lock = lock_catalog(LOCK_SHARED);
	defer(unlock, lock);

	auto user = catalog_find_user(&share()->db->catalog, &name, false);
	if (! user)
		error("auth: user '{str}' not found", &name);

	return user;
}

hot static inline User*
auth_token(Auth* self, Str* token, bool trusted, int64_t* expire)
{
	*expire = -1;

	// no token
	if (str_empty(token))
	{
		// allow only for trusted connections (localhost)
		if (! trusted)
			error("auth: authentication token is missing");

		if (self->amelie)
			return self->amelie;

		// set superuser
		auto lock = lock_catalog(LOCK_SHARED);
		defer(unlock, lock);

		Str name;
		str_set(&name, "amelie", 6);
		self->amelie = catalog_find_user(&share()->db->catalog, &name, true);
		return self->amelie;
	}

	// basic (only local)
	if (str_is_prefix_case(token, "Basic ", 6))
	{
		// allow only for trusted connections (localhost)
		if (! trusted)
			error("auth: basic token allowed only for trusted connections");

		return auth_basic(self, token);
	}

	// jwt
	if (str_is_prefix_case(token, "Bearer ", 7))
		return auth_jwt(self, token, expire);

	error("auth: invalid token");
	return NULL;
}

static inline User*
auth_run(Auth* self, Client* client)
{
	auto readahead = &client->readahead;
	auto http      = &client->request;

	// todo: add timeouts

	// read header
	auto eof = http_read(http, readahead, true);
	if (unlikely(eof))
		return NULL;

	// read content
	auto limit = opt_int_of(&config()->recv);
	auto limit_reached =
		http_read_content_limit(http, readahead, &http->content, limit);
	if (unlikely(limit_reached))
	{
		// 413 Payload Too Large (disconnect)
		client_413(client);
		return NULL;
	}

	// token
	Str token;
	str_init(&token);
	auto header = http_find(http, "Authorization", 13);
	if (header)
		token = header->value;

	// authenticate user
	auto    trusted = client->token.trusted;
	int64_t expire;
	User*   user = NULL;
	auto on_error = error_catch
	(
		user = auth_token(self, &token, trusted, &expire);

		// superuser can connect only from localhost/unixsocket (trusted source)
		if (user->config->superuser && !trusted)
			error("auth: superuser can connect only from localhost");
	);

	if (on_error)
	{
		// 401 Unauthorized (disconnect)
		client_401(client);
		return NULL;
	}

	// set client token
	token_set(&client->token, &token, expire, user);
	return user;
}

User*
auth(Auth* self, Client* client)
{
	User* user = NULL;
	auto on_error = error_catch
	(
		client_attach(client);
		client_accept(client);
		user = auth_run(self, client);
	);

	client_detach(client);

	if (on_error || !user)
	{
		client_close(client);
		return NULL;
	}

	return user;
}
