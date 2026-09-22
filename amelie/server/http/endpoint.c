
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
	OptsDef defs[] =
	{
		// protocol
		{ "proto",        OPT_INT,     OPT_C,              &self->proto,        NULL, PROTO_HTTP },
		// auth
		{ "user",         OPT_STRING,  OPT_C,              &self->user,         NULL, 0          },
		{ "token",        OPT_STRING,  OPT_C|OPT_A,        &self->token,        NULL, 0          },
		{ "token_type",   OPT_INT,     OPT_C,              &self->token_type,   NULL, TOKEN_NONE },
		// host
		{ "host",         OPT_STRING,  OPT_C,              &self->host,         NULL, 0          },
		{ "port",         OPT_INT,     OPT_C,              &self->port,         NULL, 8080       },
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
		{ "endpoint",     OPT_STRING,  OPT_C,              &self->endpoint,     NULL, 0          },
		// context
		{ "timezone",     OPT_STRING,  OPT_C|OPT_A|OPT_AE, &self->timezone,     NULL, 0          },
		{ "time",         OPT_INT,     OPT_E,              &self->time,         NULL, 0          },
		{ "seed",         OPT_INT,     OPT_E,              &self->seed,         NULL, 0          },
		// operations
		{ "feed",         OPT_STRING,  OPT_C|OPT_A|OPT_AE, &self->feed,         NULL, 0          },
		{ "copy",         OPT_STRING,  OPT_C|OPT_A|OPT_AE, &self->copy,         NULL, 0          },
		{ "mcp",          OPT_BOOL,    OPT_C|OPT_A|OPT_AE, &self->mcp,          NULL, false      },
		// misc
		{ "id",           OPT_JSON,    OPT_E,              &self->id,           NULL, 0          },
		{ "trusted",      OPT_BOOL,    OPT_E,              &self->trusted,      NULL, false      },
		{ "debug",        OPT_BOOL,    OPT_C,              &self->debug,        NULL, false      },
		{  NULL,          0,           0,                   NULL,               NULL, 0          },
	};
	opts_define(&self->opts, defs);
}

void
endpoint_free(Endpoint* self)
{
	opts_free(&self->opts);
}

void
endpoint_reset(Endpoint* self)
{
	list_foreach(&self->opts.list)
	{
		auto opt = list_at(Opt, link);
		if (opt->type == OPT_STRING || opt->type == OPT_JSON)
			str_free(&opt->string);
		else
			opt->integer = 0;
	}
	self->port.integer = 8080;
}

void
endpoint_copy(Endpoint* self, Endpoint* from)
{
	opts_copy(&self->opts, &from->opts);
}

void
endpoint_read(Endpoint* self, uint8_t** pos)
{
	opts_set_json(&self->opts, pos);
}

void
endpoint_write(Endpoint* self, Buf* buf)
{
	auto opts = opts_list_persistent(&self->opts);
	defer_buf(opts);
	buf_write_buf(buf, opts);
}

void
endpoint_auth(Endpoint* self)
{
	auto token = opt_string_of(&self->token);
	auto user  = opt_string_of(&self->user);

	// no token
	if (str_empty(token))
	{
		// no user
		if (str_empty(user))
		{
			opt_int_set(&self->token_type, TOKEN_NONE);
			return;
		}

		// create basic token (password is empty)
		Str password;
		str_init(&password);
		auto buf = basic_encode(user, &password);
		defer_buf(buf);

		opt_string_set_buf(&self->token, buf);
		opt_int_set(&self->token_type, TOKEN_BASIC);
		return;
	}

	// set token type and update user name
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

		opt_string_set(&self->user, &sub);
		opt_int_set(&self->token_type, TOKEN_JWT);
		return;
	}

	// read basic token
	Str basic_user;
	Str basic_password;
	auto buf = basic_decode(token, &basic_user, &basic_password);
	defer_buf(buf);

	opt_string_set(&self->user, &basic_user);
	opt_int_set(&self->token_type, TOKEN_BASIC);
}
