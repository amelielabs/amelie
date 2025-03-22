
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
jwt_encode_init(JwtEncode* self)
{
	json_init(&self->json);
}

void
jwt_encode_free(JwtEncode* self)
{
	json_free(&self->json);
}

void
jwt_encode_reset(JwtEncode* self)
{
	json_reset(&self->json);
}

static inline void
jwt_encode_validate(JwtEncode* self, Str* header, Str* payload)
{
	// parse header json
	json_parse(&self->json, header, NULL);

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

	// parse payload json (do not check fields)
	json_reset(&self->json);
	json_parse(&self->json, payload, NULL);
}

Buf*
jwt_encode(JwtEncode* self, Str* header, Str* payload, Str* secret)
{
	// validate data
	jwt_encode_validate(self, header, payload);

	// b64url(header).b64url(payload).b64url(hmacsha256(b64url(header).b64url(payload)))
	auto jwt = buf_create();

	// do baseu64url encoding of header and payload
	base64url_encode(jwt, header);
	buf_write(jwt, ".", 1);
	base64url_encode(jwt, payload);

	// create digest using secret
	uint8_t digest[32];
	HMAC(EVP_sha256(), str_of(secret), str_size(secret),
	     jwt->start, buf_size(jwt),
	     digest, NULL);

	// append base64url encoded digest string
	buf_write(jwt, ".", 1);
	Str digest_str;
	str_set_u8(&digest_str, digest, 32);
	base64url_encode(jwt, &digest_str);
	return jwt;
}
