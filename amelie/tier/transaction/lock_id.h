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
	LOCK_SHARED,
	LOCK_SHARED_RW,
	LOCK_EXCLUSIVE_RO,
	LOCK_EXCLUSIVE,
	LOCK_CALL,
	LOCK_MAX
} LockId;

static inline void
lock_id_encode(Buf* buf, LockId id)
{
	switch (id) {
	case LOCK_SHARED:
		encode_raw(buf, "shared", 6);
		break;
	case LOCK_SHARED_RW:
		encode_raw(buf, "shared_rw", 9);
		break;
	case LOCK_EXCLUSIVE_RO:
		encode_raw(buf, "exclusive_ro", 12);
		break;
	case LOCK_EXCLUSIVE:
		encode_raw(buf, "exclusive", 9);
		break;
	case LOCK_CALL:
		encode_raw(buf, "call", 4);
		break;
	default:
		abort();
	}
}
