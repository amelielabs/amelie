
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>

#include <openssl/hmac.h>
#include <openssl/evp.h>

void
auth_state_init(AuthState* self)
{
	self->data = NULL;
	str_init(&self->header);
	str_init(&self->payload);
	str_init(&self->digest);
	str_init(&self->digest_origin);
	json_init(&self->json);
}

void
auth_state_free(AuthState* self)
{
	if (self->data)
		buf_free(self->data);
	json_free(&self->json);
}

void
auth_state_reset(AuthState* self)
{
	if (self->data)
		buf_reset(self->data);
	else
		self->data = buf_create();
	str_init(&self->header);
	str_init(&self->payload);
	str_init(&self->digest);
	str_init(&self->digest_origin);
	json_reset(&self->json);
}

hot void
auth_state_read(AuthState* self, Str* token)
{
	// parse authentication token
	auto digest = &self->digest;

	// <Bearer> <header.payload.digest>
	*digest = *token;
	if (unlikely(! str_compare_raw_prefix(digest, "Bearer ", 7)))
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
	error("auth: failed to parse token");
}

hot void
auth_state_read_header(AuthState* self)
{
	// convert header from base64url
	base64url_decode(self->data, &self->header);

	// parse json
	Str text;
	buf_str(self->data, &text);
	json_parse(&self->json, global()->timezone, &text, NULL);

	// decode header fields
	Str alg;
	Str typ;
	uint8_t* pos = self->json.buf_data.start;
	Decode map[] =
	{
		{ DECODE_STRING, "alg", &alg },
		{ DECODE_STRING, "typ", &typ },
		{ 0,              NULL, NULL },
	};
	decode_map(map, "auth", &pos);

	// alg
	if (unlikely(! str_strncasecmp(&alg, "HS256", 5)))
		error("auth: unsupported header alg field: %.*s", str_size(&alg),
		      str_of(&alg));

	// type
	if (unlikely(! str_strncasecmp(&typ, "JWT", 3)))
		error("auth: unsupported header type field: %.*s", str_size(&typ),
		      str_of(&typ));
}

hot void
auth_state_read_payload(AuthState* self, Str* role)
{
	// convert payload from base64url
	buf_reset(self->data);
	base64url_decode(self->data, &self->payload);

	// parse json
	Str text;
	buf_str(self->data, &text);
	json_reset(&self->json);
	json_parse(&self->json, global()->timezone, &text, NULL);

	// decode payload fields
	uint8_t* pos = self->json.buf_data.start;
	Decode map[] =
	{
		{ DECODE_STRING, "sub", role },
		{ 0,              NULL, NULL },
	};
	decode_map(map, "auth", &pos);
}

hot void
auth_state_validate(AuthState* self, Str* secret)
{
	// decode digest from base64url
	buf_reset(self->data);
	base64url_decode(self->data, &self->digest);
	if (unlikely(buf_size(self->data) != 32))
		error("auth: digest has unexpected size");

	// HMACSHA256
	uint8_t digest[32];
	HMAC(EVP_sha256(), str_of(secret), str_size(secret),
	     str_u8(&self->digest_origin),
	     str_size(&self->digest_origin),
	     digest, NULL);

	if (memcmp(self->data->start, digest, 32) != 0)
		error("auth: digest mismatch");
}
