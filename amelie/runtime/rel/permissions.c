
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_rel.h>

typedef struct Permission Permission;

struct Permission
{
	uint32_t id;
	char*    name;
	int      name_size;
};

static Permission perms[] =
{
	{ PERM_NONE,            "none",            4  },

	// System
	{ PERM_SYSTEM,          "system",          6  },

	// Grants
	{ PERM_GRANT,           "grant",           5  },

	// DDL
	{ PERM_CREATE_USER,     "create_user",     11 },
	{ PERM_CREATE_TOKEN,    "create_token",    12 },
	{ PERM_CREATE_TABLE,    "create_table",    12 },
	{ PERM_CREATE_BRANCH,   "create_branch",   13 },
	{ PERM_CREATE_FUNCTION, "create_function", 15 },
	{ PERM_CREATE_STREAM,   "create_stream",   13 },

	// DML
	{ PERM_INSERT,          "insert",          6  },
	{ PERM_UPDATE,          "update",          6  },
	{ PERM_DELETE,          "delete",          6  },

	// Query
	{ PERM_SELECT,          "select",          6  },

	// UDF
	{ PERM_EXECUTE,         "execute",         7  },

	// Connections
	{ PERM_CONNECT,         "connect",         7  },
	{ PERM_BACKUP,          "backup",          6  },
	{ PERM_REPLICA,         "replica",         7  },

	// all
	{ PERM_ALL,             "all",             3  },
};

int
permission_of(Str* name, uint32_t* id)
{
	*id = PERM_NONE;
	auto perm = &perms[0];
	for (;; perm++)
	{
		if (str_is_case(name, perm->name, perm->name_size))
		{
			*id = perm->id;
			return 0;
		}
		if (perm->id == PERM_ALL)
			break;
	}
	return -1;
}

char*
permission_name_of(uint32_t id)
{
	if (! id)
		return perms[0].name;
	auto at = __builtin_ffsl(id);
	return perms[at].name;
}

uint32_t
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

uint32_t
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
			if (user)
				error("relation %.*s.%.*s: does not support '%s' grant",
				      str_size(user), str_of(user),
				      str_size(name), str_of(name),
				      id_name);
			else
				error("relation %.*s: does not support '%s' grant",
				      str_size(name), str_of(name),
				      id_name);
		}
	}
	return permissions;
}
