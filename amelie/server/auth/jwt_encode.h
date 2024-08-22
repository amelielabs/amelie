#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct JwtEncode JwtEncode;

struct JwtEncode
{
	Json json;
};

void jwt_encode_init(JwtEncode*);
void jwt_encode_free(JwtEncode*);
void jwt_encode_reset(JwtEncode*);
Buf* jwt_encode(JwtEncode*, Str*, Str*, Str*);
