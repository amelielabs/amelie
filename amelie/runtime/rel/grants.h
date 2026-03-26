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

typedef struct Grant  Grant;
typedef struct Grants Grants;

enum
{
	GRANT_NONE     = 0u,
	GRANT_SELECT   = 1u << 0,
	GRANT_INSERT   = 1u << 1,
	GRANT_UPDATE   = 1u << 2,
	GRANT_DELETE   = 1u << 3,
	GRANT_TRUNCATE = 1u << 4,
	GRANT_ALL      = 0xFFFFFFFFu
};

struct Grant
{
	uint32_t permissions;
	int      name_size;
	uint8_t  name[];
};

struct Grants
{
	Buf list;
};

static inline const char*
grant_of(uint32_t permission)
{
	switch (permission) {
	case GRANT_NONE:     return "none";
	case GRANT_SELECT:   return "select";
	case GRANT_INSERT:   return "insert";
	case GRANT_UPDATE:   return "update";
	case GRANT_DELETE:   return "delete";
	case GRANT_TRUNCATE: return "truncate";
	case GRANT_ALL:      return "all";
	}
	abort();
	return NULL;
}

static inline int
grant_of_id(Str* name, uint32_t* id)
{
	*id = 0;
	if (str_is_case(name, "none", 4) || str_empty(name))
		*id = GRANT_NONE;
	else
	if (str_is_case(name, "select", 5))
		*id = GRANT_SELECT;
	else
	if (str_is_case(name, "insert", 5))
		*id = GRANT_INSERT;
	else
	if (str_is_case(name, "update", 5))
		*id = GRANT_UPDATE;
	else
	if (str_is_case(name, "delete", 5))
		*id = GRANT_DELETE;
	else
	if (str_is_case(name, "truncate", 8))
		*id = GRANT_TRUNCATE;
	else
	if (str_is_case(name, "all", 3))
		*id = GRANT_ALL;
	else
		return -1;
	return 0;
}

static inline const char*
grant_next(uint32_t* permissions)
{
	if (*permissions == GRANT_NONE)
		return "";
	if (*permissions == GRANT_ALL)
	{
		*permissions = 0;
		return "all";
	}
	if (*permissions & GRANT_SELECT)
	{
		*permissions &= ~GRANT_SELECT;
		return "select";
	}
	if (*permissions & GRANT_INSERT)
	{
		*permissions &= ~GRANT_INSERT;
		return "insert";
	}
	if (*permissions & GRANT_UPDATE)
	{
		*permissions &= ~GRANT_UPDATE;
		return "update";
	}
	if (*permissions & GRANT_DELETE)
	{
		*permissions &= ~GRANT_DELETE;
		return "delete";
	}
	if (*permissions & GRANT_TRUNCATE)
	{
		*permissions &= ~GRANT_TRUNCATE;
		return "truncate";
	}
	abort();
	return NULL;
}

static inline void
grants_init(Grants* self)
{
	buf_init(&self->list);
}

static inline void
grants_free(Grants* self)
{
	buf_free(&self->list);
}

static inline void
grants_reset(Grants* self)
{
	buf_reset(&self->list);
}

always_inline static inline Grant*
grants_first(Grants* self)
{
	return (Grant*)self->list.start;
}

always_inline static inline Grant*
grants_next(Grants* self, Grant* grant)
{
	auto next = (Grant*)(grant->name + grant->name_size);
	if ((uint8_t*)next >= self->list.position)
		return NULL;
	return next;
}

hot static inline Grant*
grants_find(Grants* self, Str* user)
{
	auto grant = grants_first(self);
	while (grant)
	{
		if (grant->name_size == str_size(user) && str_is(user, grant->name, grant->name_size))
			return grant;
		grant = grants_next(self, grant);
	}
	return NULL;
}

hot static inline Grant*
grants_add(Grants* self, Str* user, uint32_t permissions)
{
	// update existing user grants
	auto grant = grants_find(self, user);
	if (grant)
	{
		grant->permissions |= permissions;
		return grant;
	}

	// add new user
	grant = (Grant*)buf_emplace(&self->list, sizeof(Grant) + str_size(user));
	grant->permissions = permissions;
	grant->name_size = str_size(user);
	memcpy(grant->name, str_u8(user), grant->name_size);
	return grant;
}

hot static inline void
grants_remove(Grants* self, Str* user, uint32_t permissions)
{
	// update existing user grants
	auto grant = grants_find(self, user);
	if (! grant)
		return;
	grant->permissions &= ~permissions;
	if (grant->permissions)
		return;
	// remove user from the list
	auto size = sizeof(Grant) + grant->name_size;
	auto next = grants_next(self, grant);
	if (next)
		memmove(grant, next, size);
	buf_truncate(&self->list, size);
}

hot static inline bool
grants_check(Grants* self, Str* user, uint32_t permissions)
{
	auto grant = grants_find(self, user);
	if (! grant)
		return false;
	return (grant->permissions & permissions) == permissions;
}

static inline void
grants_read(Grants* self, uint8_t** pos)
{
	// [[name, permission, ...], ...]
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		json_read_array(pos);
		Str user;
		json_read_string(pos, &user);
		uint32_t permissions = GRANT_NONE;
		while (! json_read_array_end(pos))
		{
			Str name;
			json_read_string(pos, &name);
			uint32_t permission;
			if (grant_of_id(&name, &permission) == -1)
				error("grants: unrecognized permission '%.*s'",
				      str_size(&name), str_of(&name));
			permissions |= permission;
		}
		grants_add(self, &user, permissions);
	}
}

static inline void
grants_write(Grants* self, Buf* buf, int flags)
{
	// [[name, permission, ...], ...]
	unused(flags);
	encode_array(buf);
	auto grant = grants_first(self);
	while (grant)
	{
		encode_array(buf);
		encode_raw(buf, (char*)grant->name, grant->name_size);
		uint32_t permissions = grant->permissions;
		while (permissions > 0)
		{
			auto name = grant_next(&permissions);
			encode_cstr(buf, name);
		}
		encode_array_end(buf);
		grant = grants_next(self, grant);
	}
	encode_array_end(buf);
}
