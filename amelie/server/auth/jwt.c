
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

Buf*
jwt_create(Str* user, Str* secret, Timestamp* expire)
{
	if (str_empty(secret))
		error("jwt: user secret is not set");

	JwtEncode jwt;
	jwt_encode_init(&jwt);
	guard(jwt_encode_free, &jwt);

	// header
	char header[] = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
	Str header_str;
	str_set(&header_str, header, sizeof(header) - 1);

	// payload
	auto payload = buf_create();
	guard(buf_free, payload);
	encode_map(payload);

	// sub
	encode_raw(payload, "sub", 3);
	encode_string(payload, user);

	// iat
	auto iat = time_ms() / 1000;
	encode_raw(payload, "iat", 3);
	encode_integer(payload, iat);

	// exp
	if (expire)
	{
		encode_raw(payload, "exp", 3);
		auto exp = timestamp_of(expire, NULL) / 1000 / 1000;
		encode_integer(payload, exp);
	}
	encode_map_end(payload);

	// convert payload to json
	auto payload_json = buf_create();
	guard_buf(payload_json);

	uint8_t* pos = payload->start;
	json_export(payload_json, NULL, &pos);

	// create jwt
	Str payload_str;
	buf_str(payload_json, &payload_str);
	return jwt_encode(&jwt, &header_str, &payload_str, secret);
}
