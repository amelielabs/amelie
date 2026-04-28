
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

bool
catalog_grant(Catalog* self, Tr* tr,
              Str*     user,
              Str*     name,
              Str*     to,
              bool     grant,
              int64_t  perms)
{
	// PERM_GRANT
	check_user(tr, PERM_GRANT);

	// user grants
	if (str_empty(user))
	{
		// PERM_SYSTEM for system wide grants
		if (((perms & PERM_SYSTEM) > 0) ||
			((perms & PERM_CREATE_USER) > 0))
			check_user(tr, PERM_SYSTEM);
		return user_grant(self, tr, to, grant, perms, 0);
	}

	// relations
	auto write = false;
	auto rel = catalog_find(self, REL_UNDEF, user, name, true);
	switch (rel->type) {
	case REL_TABLE:
		write = table_grant(self, tr, user, name, to, grant, perms, 0);
		break;
	case REL_BRANCH:
		write = branch_grant(self, tr, user, name, to, grant, perms, 0);
		break;
	case REL_UDF:
		write = udf_grant(self, tr, user, name, to, grant, perms, 0);
		break;
	case REL_TOPIC:
		write = topic_grant(self, tr, user, name, to, grant, perms, 0);
		break;
	case REL_SUBSCRIPTION:
		write = sub_grant(self, tr, user, name, to, grant, perms, 0);
		break;
	default:
		abort();
		break;
	}
	return write;
}
