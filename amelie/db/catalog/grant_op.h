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
	encode_integer(self, DDL_GRANT);
	encode_string(self, user);
	encode_string(self, name);
	encode_string(self, to);
	encode_bool(self, grant);
	encode_integer(self, perms);
	encode_array_end(self);
	return offset;
}

static inline void
grant_op_grant_read(uint8_t* op, Str* user, Str* name, Str* to, bool* grant, int64_t* perms)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_GRANT);
	json_read_string(&op, user);
	json_read_string(&op, name);
	json_read_string(&op, to);
	json_read_bool(&op, grant);
	json_read_integer(&op, perms);
	json_read_array_end(&op);
}
