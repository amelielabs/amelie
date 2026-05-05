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

typedef struct Catalog Catalog;
typedef struct User User;

struct User
{
	Rel         rel;
	int64_t     revoked_at;
	UserConfig* config;
};

bool user_create(Catalog*, Tr*, UserConfig*, bool);
bool user_drop(Catalog*, Tr*, Str*, bool, bool);
bool user_rename(Catalog*, Tr*, Str*, Str*, bool);
bool user_revoke(Catalog*, Tr*, Str*, Str*, bool);
bool user_grant(Catalog*, Tr*, Str*, bool, uint32_t, bool);

always_inline static inline User*
user_of(Rel* self)
{
	return (User*)self;
}

static inline void
user_sync(User* self)
{
	if (str_empty(&self->config->revoked_at))
	{
		self->revoked_at = 0;
		return;
	}
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_set(&ts, &self->config->revoked_at);
	auto time = timestamp_get_unixtime(&ts, runtime()->timezone);
	self->revoked_at = time / 1000 / 1000;
}
