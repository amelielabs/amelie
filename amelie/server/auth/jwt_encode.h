#pragma once

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

typedef struct JwtEncode JwtEncode;

struct JwtEncode
{
	Json json;
};

void jwt_encode_init(JwtEncode*);
void jwt_encode_free(JwtEncode*);
void jwt_encode_reset(JwtEncode*);
Buf* jwt_encode(JwtEncode*, Str*, Str*, Str*);
