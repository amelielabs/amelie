
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
	jwt_decode_reset(&self->jwt);
}

hot static inline void
auth_basic(Auth* self, Endpoint* endpoint)
{
	unused(self);

	// parse basic token
	auto token = opt_string_of(&endpoint->token);
	Str basic = *token;
	str_advance(&basic, 6);

	Str name;
	Str password;
	auto buf = basic_decode(&basic, &name, &password);
	defer_buf(buf);

	// set user
	opt_string_set(&endpoint->user, &name);
}

hot static inline void
auth_jwt(Auth* self, Db* db, Endpoint* endpoint, uint64_t* expire)
{
	// parse token
	auto token = opt_string_of(&endpoint->token);
	Str bearer = *token;
	str_advance(&bearer, 7);

	auto jwt = &self->jwt;
	jwt_decode(jwt, &bearer);

	// validate header, payload and read role, iat and exp
	int64_t iat;
	int64_t exp;
	Str     sub;
	str_init(&sub);
	jwt_decode_data(jwt, &sub, &iat, &exp);

	// validate sub against db name
	if (! str_compare(&sub, db->rel.name))
		error("auth: db '{str}' token sub does not match db", db->rel.name);

	// validate issue/expire fields
	if (!iat || !exp || iat > exp)
		error("auth: db '{str}' token has invalid iat/exp fields", &sub);

	// ensure token has not expired
	int64_t now = time_us();
	if (now >= exp)
		error("auth: db '{str}' token has expired", &sub);

	// check issued time against db revoked_at
	if (iat <= (int64_t)db->inv_revoked)
		error("auth: db '{str}' token has been revoked", &sub);

	// validate token using secret
	auto secret = opt_string_of(&state()->secret);
	if (! jwt_decode_validate(jwt, secret))
		error("auth: db '{str}' token is invalid", &sub);

	*expire = exp;
}

hot static inline void
auth_db(Auth* self, Db* db, Endpoint* endpoint, uint64_t* expire)
{
	auto trusted = opt_int_of(&endpoint->trusted);
	*expire = UINT64_MAX;

	// no token
	auto token = opt_string_of(&endpoint->token);
	if (str_empty(token))
	{
		// allow only for trusted connections (localhost)
		if (! trusted)
			error("auth: authentication token is missing");
		return;
	}

	// basic (only local)
	if (str_is_prefix_case(token, "Basic ", 6))
	{
		// allow only for trusted connections (localhost)
		if (! trusted)
			error("auth: basic token allowed only for trusted connections");

		auth_basic(self, endpoint);
		return;
	}

	// jwt
	if (str_is_prefix_case(token, "Bearer ", 7))
	{
		auth_jwt(self, db, endpoint, expire);
		return;
	}

	error("auth: invalid token");
}

Db*
auth_run(Auth* self, Endpoint* endpoint, Token* token)
{
	uint64_t seq_secret = opt_int_of(&state()->secret_seq);

	// find database
	auto db_name = opt_string_of(&endpoint->db);
	auto db = catalog_find_db(&share()->storage->databases, db_name, true);

	// validate previous token
	auto token_str = opt_string_of(&endpoint->token);
	if (likely(! token_empty(token)))
	{
		auto now = time_us();
		if (likely(token->seq_secret == seq_secret &&
		           token->expire > now &&
		           str_compare(db_name, &token->db) &&
		           str_compare(token_str, &token->token)))
		{
			if (db->inv_seq == token->seq_db)
				return db;
		}

		// invalidate token
		token_reset(token);
	}

	// authenticate token
	uint64_t expire;
	auth_db(self, db, endpoint, &expire);

	// system can connect only from localhost/unixsocket (trusted source)
	auto trusted = opt_int_of(&endpoint->trusted);
	if (db->config->system && !trusted)
		error("auth: system can connect only from localhost");

	// set token
	token_set(token, token_str, &db->config->name, db->inv_seq,
	          seq_secret, expire);
	return db;
}
