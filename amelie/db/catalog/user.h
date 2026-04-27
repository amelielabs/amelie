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

typedef struct User User;

struct User
{
	Rel         rel;
	int64_t     revoked_at;
	UserConfig* config;
};

static inline void
user_free(User* self, bool drop)
{
	unused(drop);
	if (self->config)
		user_config_free(self->config);
	am_free(self);
}

static inline void
user_show(User* self, Buf* buf, int flags)
{
	user_config_write(self->config, buf, flags);
}

static inline User*
user_allocate(UserConfig* config)
{
	User* self = am_malloc(sizeof(User));
	self->revoked_at = 0;
	self->config     = user_config_copy(config);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_USER);
	rel_set_user(rel, &self->config->parent);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)user_show);
	rel_set_free(rel, (RelFree)user_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
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

static inline User*
user_of(Rel* self)
{
	return (User*)self;
}

static inline void
user_permission_error(User* self)
{
	error("user '%.*s': permission denied", str_size(&self->config->name),
	      str_of(&self->config->name));
}

static inline bool
user_owns(User* self, Rel* rel)
{
	return self->config->superuser ||
	       str_compare(&self->config->name, rel->user);
}

static inline void
user_check(User* self, uint32_t perms)
{
	if (self->config->superuser)
		return;
	if (! grants_check_first(&self->config->grants, perms))
		user_permission_error(self);
}

hot static inline void
user_check_permission(User* self, Rel* rel, uint32_t perms)
{
	// user must have the permissions on the relation
	if (self->config->superuser)
		return;

	if (unlikely(! rel->grants))
		return;

	// owner
	if (str_compare(&self->config->name, rel->user))
		return;

	// check permissions
	if (likely(grants_check(rel->grants, &self->config->name, perms)))
		return;

	rel_permission_error(rel);
}

hot static inline void
user_check_access(User* self, Access* access)
{
	// check permissions on the relations
	if (self->config->superuser)
		return;
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		if (record->perm == PERM_NONE)
			continue;
		user_check_permission(self, record->rel, record->perm);
	}
}
