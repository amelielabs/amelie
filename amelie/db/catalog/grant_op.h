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

static inline int
grant_op_grant(Buf* self, Str* user, Str* name, Str* to, bool grant, uint32_t perms)
{
	// [op, user, name, to, grant, perms]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_GRANT);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, to);
	encode_bool(self, grant);
	encode_int(self, perms);
	encode_array_end(self);
	return offset;
}

static inline void
grant_op_grant_read(uint8_t* op, Str* user, Str* name, Str* to, bool* grant, int64_t* perms)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_GRANT);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, to);
	unpack_bool(&op, grant);
	unpack_int(&op, perms);
	unpack_array_end(&op);
}
