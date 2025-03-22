
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

#include <openssl/hmac.h>
#include <openssl/evp.h>

void
jwt_decode_init(JwtDecode* self)
{
	str_init(&self->header);
	str_init(&self->payload);
	str_init(&self->digest);
	str_init(&self->digest_origin);
	buf_init(&self->data);
	json_init(&self->json);
}

void
jwt_decode_free(JwtDecode* self)
{
	buf_free(&self->data);
	json_free(&self->json);
}

void
jwt_decode_reset(JwtDecode* self)
{
	buf_reset(&self->data);
	str_init(&self->header);
	str_init(&self->payload);
	str_init(&self->digest);
	str_init(&self->digest_origin);
	json_reset(&self->json);
}

hot void
jwt_decode(JwtDecode* self, Str* token)
{
	// parse authentication token
	auto digest = &self->digest;

	// <Bearer> <header.payload.digest>
	*digest = *token;
	if (unlikely(! str_is_prefix(digest, "Bearer ", 7)))
		goto error_jwt;
	str_advance(digest, 7);

	// header
	if (unlikely(! str_split(digest, &self->header, '.')))
		goto error_jwt;
	if (unlikely(str_empty(&self->header)))
		goto error_jwt;
	str_advance(digest, str_size(&self->header) + 1);

	// payload
	if (unlikely(! str_split(digest, &self->payload, '.')))
		goto error_jwt;
	if (unlikely(str_empty(&self->payload)))
		goto error_jwt;
	str_advance(digest, str_size(&self->payload) + 1);

	// save string used for the digest (header.payload)
	auto start = token->pos + 7;
	str_set(&self->digest_origin, start, (digest->pos - start) - 1);

	// digest
	if (unlikely(str_empty(&self->digest)))
		goto error_jwt;
	return;

error_jwt:
	error("jwt: failed to parse token");
}

hot static void
jwt_decode_header(JwtDecode* self)
{
	// convert header from base64url
	base64url_decode(&self->data, &self->header);

	// parse json
	Str text;
	buf_str(&self->data, &text);
	json_parse(&self->json, &text, NULL);

	// decode header fields
	Str alg;
	str_init(&alg);
	Str typ;
	str_init(&typ);
	uint8_t* pos = self->json.buf_data.start;
	Decode obj[] =
	{
		{ DECODE_STRING_READ, "alg", &alg },
		{ DECODE_STRING_READ, "typ", &typ },
		{ 0,                   NULL, NULL },
	};
	decode_obj(obj, "jwt", &pos);

	// alg
	if (unlikely(! str_is_case(&alg, "HS256", 5)))
		error("jwt: unsupported header alg field: %.*s", str_size(&alg),
		      str_of(&alg));

	// type
	if (unlikely(! str_is_case(&typ, "JWT", 3)))
		error("jwt: unsupported header type field: %.*s", str_size(&typ),
		      str_of(&typ));
}

hot static void
jwt_decode_payload(JwtDecode* self, Str* sub,
                   int64_t*   iat,
                   int64_t*   exp)
{
	// convert payload from base64url
	buf_reset(&self->data);
	base64url_decode(&self->data, &self->payload);

	// parse json
	Str text;
	buf_str(&self->data, &text);
	json_reset(&self->json);
	json_parse(&self->json, &text, NULL);

	// decode payload fields
	uint8_t* pos = self->json.buf_data.start;
	Decode obj[] =
	{
		{ DECODE_STRING_READ, "sub", sub  },
		{ DECODE_INT,         "iat", iat  },
		{ DECODE_INT,         "exp", exp  },
		{ 0,                   NULL, NULL },
	};
	decode_obj(obj, "jwt", &pos);
}

hot void
jwt_decode_data(JwtDecode* self, Str* sub,
                int64_t*   iat,
                int64_t*   exp)
{
	jwt_decode_header(self);
	jwt_decode_payload(self, sub, iat, exp);
}

hot void
jwt_decode_validate(JwtDecode* self, Str* secret)
{
	// decode digest from base64url
	buf_reset(&self->data);
	base64url_decode(&self->data, &self->digest);
	if (unlikely(buf_size(&self->data) != 32))
		error("jwt: digest has unexpected size");

	// HMACSHA256
	uint8_t digest[32];
	HMAC(EVP_sha256(), str_of(secret), str_size(secret),
	     str_u8(&self->digest_origin),
	     str_size(&self->digest_origin),
	     digest, NULL);

	if (memcmp(self->data.start, digest, 32) != 0)
		error("jwt: digest mismatch");
}
