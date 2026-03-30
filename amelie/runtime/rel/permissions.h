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
	PERM_EXECUTE  = 1u << 5,
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
	if (str_is_case(name, "select", 5))
		*id = PERM_SELECT;
	else
	if (str_is_case(name, "insert", 5))
		*id = PERM_INSERT;
	else
	if (str_is_case(name, "update", 5))
		*id = PERM_UPDATE;
	else
	if (str_is_case(name, "delete", 5))
		*id = PERM_DELETE;
	else
	if (str_is_case(name, "truncate", 8))
		*id = PERM_TRUNCATE;
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

static inline const char*
permission_next(uint32_t* permissions)
{
	if (*permissions == PERM_NONE)
		return "";
	if (*permissions == PERM_ALL)
	{
		*permissions = 0;
		return "all";
	}
	if (*permissions & PERM_SELECT)
	{
		*permissions &= ~PERM_SELECT;
		return "select";
	}
	if (*permissions & PERM_INSERT)
	{
		*permissions &= ~PERM_INSERT;
		return "insert";
	}
	if (*permissions & PERM_UPDATE)
	{
		*permissions &= ~PERM_UPDATE;
		return "update";
	}
	if (*permissions & PERM_DELETE)
	{
		*permissions &= ~PERM_DELETE;
		return "delete";
	}
	if (*permissions & PERM_TRUNCATE)
	{
		*permissions &= ~PERM_TRUNCATE;
		return "truncate";
	}
	if (*permissions & PERM_EXECUTE)
	{
		*permissions &= ~PERM_EXECUTE;
		return "execute";
	}
	abort();
	return NULL;
}
