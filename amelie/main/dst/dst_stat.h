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

typedef struct DstStatOp DstStatOp;
typedef struct DstStat   DstStat;

enum
{
	DST_STAT_INSERT,
	DST_STAT_INSERT_VECTOR,
	DST_STAT_INSERT_CLONE,

	DST_STAT_UPSERT,
	DST_STAT_UPSERT_VECTOR,
	DST_STAT_UPSERT_CLONE,

	DST_STAT_UPDATE,
	DST_STAT_UPDATE_VECTOR,
	DST_STAT_UPDATE_CLONE,

	DST_STAT_DELETE,
	DST_STAT_DELETE_VECTOR,
	DST_STAT_DELETE_CLONE,

	DST_STAT_PUBLISH,

	DST_STAT_CREATE_USER,
	DST_STAT_CREATE_TABLE,
	DST_STAT_CREATE_TABLE_VECTOR,
	DST_STAT_CREATE_INDEX,
	DST_STAT_CREATE_CLONE,
	DST_STAT_CREATE_TOPIC,
	DST_STAT_CREATE_SUBSCRIPTION,
	DST_STAT_DROP,

	DST_STAT_ERRORS_INJECTED,
	DST_STAT_ROLLBACK,

	DST_STAT_BACKUP,
	DST_STAT_CHECKPOINT,
	DST_STAT_RESTART,
	DST_STAT_VALIDATION,
	DST_STAT_SQL,

	DST_STAT_MAX
};

struct DstStat
{
	int ops[DST_STAT_MAX];
};

static inline void
dst_stat_init(DstStat* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
dst_stat(DstStat* self, int op)
{
	self->ops[op]++;
}

void dst_stat_print(DstStat*);
