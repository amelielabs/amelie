
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
#include <amelie_token.h>
#include <amelie_http.h>

void
endpoint_init(Endpoint* self)
{
	opts_init(&self->opts);
	OptsDef defs[25] =
	{
		// protocol
		{ "proto",        OPT_U32,     OPT_C,              &self->proto,        NULL, PROTO_HTTP },
		// auth
		{ "user",         OPT_STRING,  OPT_C,              &self->user,         NULL, 0          },
		{ "token",        OPT_STRING,  OPT_C|OPT_A,        &self->token,        NULL, 0          },
		{ "token_type",   OPT_U32,     OPT_C,              &self->token_type,   NULL, TOKEN_NONE },
		// host
		{ "host",         OPT_STRING,  OPT_C,              &self->host,         NULL, 0          },
		{ "port",         OPT_U32,     OPT_C,              &self->port,         NULL, 8080       },
		// repository
		{ "path",         OPT_STRING,  OPT_C,              &self->path,         NULL, 0          },
		// tls
		{ "tls_capath",   OPT_STRING,  OPT_C|OPT_A,        &self->tls_capath,   NULL, 0          },
		{ "tls_ca",       OPT_STRING,  OPT_C|OPT_A,        &self->tls_ca,       NULL, 0          },
		{ "tls_cert",     OPT_STRING,  OPT_C|OPT_A,        &self->tls_cert,     NULL, 0          },
		{ "tls_key",      OPT_STRING,  OPT_C|OPT_A,        &self->tls_key,      NULL, 0          },
		{ "tls_server",   OPT_STRING,  OPT_C|OPT_A,        &self->tls_server,   NULL, 0          },
		// endpoint
		{ "uri",          OPT_STRING,  OPT_E,              &self->uri,          NULL, 0          },
		{ "content_type", OPT_STRING,  OPT_C|OPT_A,        &self->content_type, NULL, 0          },
		{ "accept",       OPT_STRING,  OPT_C|OPT_A,        &self->accept,       NULL, 0          },
		// relation
		{ "db",           OPT_STRING,  OPT_C,              &self->db,           NULL, 0          },
		{ "db_id",        OPT_UUID,    OPT_E,              &self->db_id,        NULL, 0          },
		{ "rel",          OPT_STRING,  OPT_C,              &self->rel,          NULL, 0          },
		// context
		{ "timezone",     OPT_STRING,  OPT_C|OPT_A|OPT_AE, &self->timezone,     NULL, 0          },
		{ "time",         OPT_U64,     OPT_E,              &self->time,         NULL, 0          },
		{ "seed",         OPT_U64,     OPT_E,              &self->seed,         NULL, 0          },
		{ "id",           OPT_STRING,  OPT_C|OPT_A|OPT_AE, &self->id,           NULL, 0          },
		// misc
		{ "jsonrpc",      OPT_JSON,    OPT_E,              &self->jsonrpc,      NULL, 0          },
		{ "trusted",      OPT_BOOL,    OPT_E,              &self->trusted,      NULL, false      },
		{ "debug",        OPT_BOOL,    OPT_C,              &self->debug,        NULL, false      },
	};
	opts_define(&self->opts, defs, 25);
}

void
endpoint_free(Endpoint* self)
{
	opts_free(&self->opts);
}

void
endpoint_reset(Endpoint* self)
{
	self->port = 8080;
	opts_reset(&self->opts);
}

void
endpoint_auth(Endpoint* self)
{
	auto token = &self->token;
	auto db    = &self->db;

	// no token
	if (str_empty(token))
	{
		// no db
		if (str_empty(db))
		{
			self->token_type = TOKEN_NONE;
			return;
		}

		// create basic token (password is empty)
		Str password;
		str_init(&password);
		auto buf = basic_encode(db, &password);
		defer_buf(buf);

		self->token_type = TOKEN_BASIC;
		str_assign_raw(&self->token, buf->start, buf_size(buf));
		return;
	}

	// set token type and update db name
	if (str_chr(token, '.'))
	{
		// read jwt token
		JwtDecode jwt;
		jwt_decode_init(&jwt);
		defer(jwt_decode_free, &jwt);
		jwt_decode(&jwt, token);

		int64_t iat;
		int64_t exp;
		Str     sub;
		str_init(&sub);
		jwt_decode_data(&jwt, &sub, &iat, &exp);

		self->token_type = TOKEN_JWT;
		str_assign(&self->db, &sub);
		return;
	}

	// read basic token
	Str basic_db;
	Str basic_password;
	auto buf = basic_decode(token, &basic_db, &basic_password);
	defer_buf(buf);

	self->token_type = TOKEN_BASIC;
	str_assign(&self->db, &basic_db);
}
