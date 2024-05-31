#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef enum
{
	REPL_PRIMARY,
	REPL_REPLICA
} ReplRole;

static inline char*
repl_role_of(ReplRole role)
{
	switch (role) {
	case REPL_PRIMARY: return "primary";
	case REPL_REPLICA: return "replica";
	}
	return NULL;
}

static inline int
repl_role_read(Str* str)
{
	if (str_compare_raw(str, "primary", 7))
		return REPL_PRIMARY;
	if (str_compare_raw(str, "replica", 7))
		return REPL_REPLICA;
	return -1;
}
