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
	LOCK_SHARED_RW,
	LOCK_EXCLUSIVE_RO,
	LOCK_EXCLUSIVE,
	LOCK_CALL,
	LOCK_MAX
} LockId;

static inline LockId
lock_id_of(Str* name)
{
	if (str_is_case(name, "none", 4))
		return LOCK_NONE;
	if (str_is_case(name, "shared", 6))
		return LOCK_SHARED;
	if (str_is_case(name, "shared_rw", 9))
		return LOCK_SHARED_RW;
	if (str_is_case(name, "exclusive_ro", 12))
		return LOCK_EXCLUSIVE_RO;
	if (str_is_case(name, "exclusive", 9))
		return LOCK_EXCLUSIVE;
	return LOCK_NONE;
}

static inline const char*
lock_id_cstr(LockId self)
{
	switch (self) {
	case LOCK_NONE:         return "none";
	case LOCK_SHARED:       return "shared";
	case LOCK_SHARED_RW:    return "shared_rw";
	case LOCK_EXCLUSIVE_RO: return "exclusive_ro";
	case LOCK_EXCLUSIVE:    return "exclusive";
	case LOCK_CALL:         return "call";
	default:
		abort();
	}
}

static inline void
lock_id_encode(Buf* buf, LockId id)
{
	encode_cstr(buf, lock_id_cstr(id));
}
