
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_compute.h>

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

hot static inline Db*
auth_jwt(Auth* self, Str* token, uint64_t* expire)
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
		error("auth: db '{str}' token has invalid iat/exp fields", &sub);

	// ensure token has not expired
	int64_t now = time_us();
	if (now >= exp)
		error("auth: db '{str}' token has expired", &sub);

	// find db
	auto db = catalog_find_db(&share()->storage->databases, &sub, false);
	if (! db)
		error("auth: db '{str}' not found", &sub);

	// check issued time against db revoked_at
	if (iat <= (int64_t)db->inv_revoked)
		error("auth: db '{str}' token has been revoked", &sub);

	// validate token using secret
	auto secret = opt_string_of(&state()->secret);
	if (! jwt_decode_validate(jwt, secret))
		error("auth: db '{str}' token is invalid", &sub);

	*expire = exp;
	return db;
}

hot static inline Db*
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

	// find db
	auto db = catalog_find_db(&share()->storage->databases, &name, false);
	if (! db)
		error("auth: db '{str}' not found", &name);

	return db;
}

hot static inline Db*
auth_token(Auth* self, Str* token, bool trusted, uint64_t* expire)
{
	*expire = UINT64_MAX;

	// no token
	if (str_empty(token))
	{
		// allow only for trusted connections (localhost)
		if (! trusted)
			error("auth: authentication token is missing");

		if (self->amelie)
			return self->amelie;

		// set system
		Str name;
		str_set(&name, "amelie", 6);
		self->amelie = catalog_find_db(&share()->storage->databases, &name, true);
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

Db*
auth_run(Auth* self, Endpoint* endpoint, Token* token)
{
	uint64_t seq_secret = opt_int_of(&state()->secret_seq);

	// validate token
	auto ref = opt_string_of(&endpoint->token);
	if (likely(! token_empty(token)))
	{
		auto now = time_us();
		if (likely(token->seq_secret == seq_secret &&
		           token->expire > now &&
		           str_compare(ref, &token->token)))
		{
			auto db = catalog_find_db(&share()->storage->databases, &token->db, true);
			if (db->inv_seq == token->seq_db)
				return db;
		}

		// invalidate token
		token_reset(token);
	}

	auto trusted = opt_int_of(&endpoint->trusted);

	// authenticate token
	uint64_t expire;
	auto db = auth_token(self, ref, trusted, &expire);

	// system can connect only from localhost/unixsocket (trusted source)
	if (db->config->system && !trusted)
		error("auth: system can connect only from localhost");

	// set token
	token_set(token, ref, &db->config->name, db->inv_seq, seq_secret, expire);
	return db;
}
