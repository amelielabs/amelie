#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct JwtDecode JwtDecode;

struct JwtDecode
{
	Str  header;
	Str  payload;
	Str  digest;
	Str  digest_origin;
	Buf  data;
	Json json;
};

void jwt_decode_init(JwtDecode*);
void jwt_decode_free(JwtDecode*);
void jwt_decode_reset(JwtDecode*);
void jwt_decode(JwtDecode*, Str*);
void jwt_decode_data(JwtDecode*, Str*, int64_t*, int64_t*);
void jwt_decode_validate(JwtDecode*, Str*);
