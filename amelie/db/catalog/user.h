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
typedef struct User    User;

struct User
{
	Rel         rel;
	Track       track;
	int64_t     revoked_at;
	UserConfig* config;
	Usage       memory;
	Catalog*    catalog;
};

bool user_create(Catalog*, Tr*, UserConfig*, bool);
bool user_drop(Catalog*, Tr*, Str*, bool, bool);
bool user_rename(Catalog*, Tr*, Str*, Str*, bool);
bool user_revoke_token(Catalog*, Tr*, Str*, Str*, bool);
bool user_grant(Catalog*, Tr*, Str*, bool, uint32_t, bool);
bool user_describe(Catalog*, Tr*, Str*, Str*, bool);
bool user_limit_set(Catalog*, Tr*, Str*, Limits*, bool);
bool user_limit_unset(Catalog*, Tr*, Str*, uint64_t, bool);
bool user_api_create(Catalog*, Tr*, Str*, Api*, bool);
bool user_api_drop(Catalog*, Tr*, Str*, Str*, bool);

always_inline static inline User*
user_of(Rel* self)
{
	return (User*)self;
}

static inline void
user_sync(User* self)
{
	Consensus commit;
	consensus_atomic_read(&self->track.consensus_commit, &commit);
	track_commit(&self->track, &commit);
}
