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

enum
{
	CIPHER_NONE,
	CIPHER_AES
};

Codec* cipher_create(CodecCache*, int, Random*, Str*, Buf*);
