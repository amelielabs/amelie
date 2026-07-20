
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_dst.h>

static const char* dst_ops[DST_STAT_MAX] =
{
	"insert",
	"insert_vector",
	"insert_clone",

	"upsert",
	"upsert_vector",
	"upsert_clone",

	"update",
	"update_vector",
	"update_clone",

	"delete",
	"delete_vector",
	"delete_clone",

	"publish",

	"create user",
	"create table",
	"create table (vector)",
	"create index",
	"create clone",
	"create topic",
	"create subscription",

	"errors injected",
	"rollback",
};

void
dst_stat_print(DstStat* self)
{
	for (auto i = 0; i < DST_STAT_MAX; i++)
		info("{-24s} {d}", dst_ops[i], self->ops[i]);
}
