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

static inline Buf*
basic_encode(Str* user, Str* password)
{
	// base64(user:password)
	auto buf = buf_create();
	defer_buf(buf);
	buf_format(buf, "{str}:{str}", user, password);

	auto basic = buf_create();
	Str str;
	buf_str(buf, &str);
	base64_encode(basic, &str);
	return basic;
}

static inline Buf*
basic_decode(Str* self, Str* user, Str* password)
{
	auto buf = buf_create();
	errdefer_buf(buf);
	base64_decode(buf, self);

	Str str;
	buf_str(buf, &str);
	if (! str_split(&str, user, ':'))
		error("basic: invalid token");

	*password = str;
	str_advance(password, str_size(user) + 1);
	return buf;
}
