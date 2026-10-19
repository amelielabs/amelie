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

typedef enum
{
	LOCK_NONE,
	LOCK_SHARED,
	LOCK_EXCLUSIVE,
	LOCK_MAX
} LockId;

static inline LockId
lock_id_of(Str* name)
{
	if (str_is_case(name, "none", 4))
		return LOCK_NONE;
	if (str_is_case(name, "shared", 6))
		return LOCK_SHARED;
	if (str_is_case(name, "exclusive", 9))
		return LOCK_EXCLUSIVE;
	return LOCK_NONE;
}

static inline const char*
lock_id_cstr(LockId self)
{
	switch (self) {
	case LOCK_NONE:      return "none";
	case LOCK_SHARED:    return "shared";
	case LOCK_EXCLUSIVE: return "exclusive";
	default:
		abort();
	}
}

static inline void
lock_id_encode(Buf* buf, LockId id)
{
	encode_cstr(buf, lock_id_cstr(id));
}
