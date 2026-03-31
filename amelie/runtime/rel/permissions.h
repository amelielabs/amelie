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

enum
{
	PERM_NONE     = 0u,
	PERM_SELECT   = 1u << 0,
	PERM_INSERT   = 1u << 1,
	PERM_UPDATE   = 1u << 2,
	PERM_DELETE   = 1u << 3,
	PERM_TRUNCATE = 1u << 4,
	PERM_BRANCH   = 1u << 5,
	PERM_EXECUTE  = 1u << 6,
	PERM_ALL      = 0xFFFFFFFFu
};

static inline const char*
permission_name_of(uint32_t id)
{
	switch (id) {
	case PERM_NONE:     return "none";
	case PERM_SELECT:   return "select";
	case PERM_INSERT:   return "insert";
	case PERM_UPDATE:   return "update";
	case PERM_DELETE:   return "delete";
	case PERM_TRUNCATE: return "truncate";
	case PERM_BRANCH:   return "branch";
	case PERM_EXECUTE:  return "execute";
	case PERM_ALL:      return "all";
	}
	abort();
	return NULL;
}

static inline int
permission_of(Str* name, uint32_t* id)
{
	*id = 0;
	if (str_is_case(name, "none", 4) || str_empty(name))
		*id = PERM_NONE;
	else
	if (str_is_case(name, "select", 6))
		*id = PERM_SELECT;
	else
	if (str_is_case(name, "insert", 6))
		*id = PERM_INSERT;
	else
	if (str_is_case(name, "update", 6))
		*id = PERM_UPDATE;
	else
	if (str_is_case(name, "delete", 6))
		*id = PERM_DELETE;
	else
	if (str_is_case(name, "truncate", 8))
		*id = PERM_TRUNCATE;
	else
	if (str_is_case(name, "branch", 6))
		*id = PERM_BRANCH;
	else
	if (str_is_case(name, "execute", 7))
		*id = PERM_EXECUTE;
	else
	if (str_is_case(name, "all", 3))
		*id = PERM_ALL;
	else
		return -1;
	return 0;
}

static inline uint32_t
permission_next(uint32_t* permissions)
{
	if (! *permissions)
		return PERM_NONE;
	if (*permissions == PERM_ALL)
	{
		*permissions = 0;
		return PERM_ALL;
	}
	auto next_bit = __builtin_ffsl(*permissions) - 1;
	auto next = 1u << next_bit;
	*permissions &= ~next;
	return next;
}

static inline uint32_t
permission_validate(Str* user, Str* name, uint32_t permissions, uint32_t mask)
{
	// ALL returns only supported permissions
	if (permissions == PERM_ALL)
		return mask;

	// validate the list against supported permissions
	auto perms = permissions;
	while (perms > 0)
	{
		auto id = permission_next(&perms);
		if (id == PERM_NONE)
			break;
		if ((id & mask) != id)
		{
			auto id_name = permission_name_of(id);
			error("relation %.*s.%.*s: does not support '%s' grant",
			      str_size(user), str_of(user),
			      str_size(name), str_of(name),
			      id_name);
		}
	}
	return permissions;
}
