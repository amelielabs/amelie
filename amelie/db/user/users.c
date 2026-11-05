
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_user.h>

void
users_init(Users*     self,
           UsersIf*   iface,
           CatalogIf* iface_catalog,
           EvalIf*    iface_eval,
           void*      iface_arg)
{
	self->iface         = iface;
	self->iface_catalog = iface_catalog;
	self->iface_eval    = iface_eval;
	self->iface_arg     = iface_arg;
	rels_init(&self->users);
}

void
users_free(Users* self)
{
	rels_free(&self->users);
}

void
users_create(Users* self)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// create amelie user
		auto user_config = user_config_allocate();
		defer(user_config_free, user_config);
		Str name;
		str_init(&name);
		str_set_cstr(&name, "amelie");
		user_config_set_name(user_config, &name);
		user_config_set_parent(user_config, &name);
		user_config_set_superuser(user_config, true);
		user_create(self, &tr, user_config, false);

		// commit
		tr_commit(&tr);
	);
	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
users_status(Users* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// users
	encode_raw(buf, "users", 5);
	encode_int(buf, self->users.list_count);

	encode_obj_end(buf);
}

bool
users_execute(Users* self, Tr* tr, uint8_t* op, int flags)
{
	// [op, ...]
	auto cmd = ddl_of(op);
	bool write = false;
	switch (cmd) {
	case USER_CREATE:
	{
		auto config = user_op_create_read(op);
		defer(user_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = user_create(self, tr, config, if_not_exists);
		break;
	}
	case USER_DROP:
	{
		Str  name;
		bool cascade;
		user_op_drop_read(op, &name, &cascade);

		auto if_exists = ddl_if_exists(flags);
		write = user_drop(self, tr, &name, if_exists, cascade);
		break;
	}
	case USER_RENAME:
	{
		Str name;
		Str name_new;
		user_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = user_rename(self, tr, &name, &name_new, if_exists);
		break;
	}
	case USER_REVOKE_TOKEN:
	{
		Str name;
		Str revoked_at;
		user_op_revoke_token_read(op, &name, &revoked_at);

		auto if_exists = ddl_if_exists(flags);
		write = user_revoke_token(self, tr, &name, &revoked_at, if_exists);
		break;
	}
	case USER_DESCRIBE:
	{
		Str name;
		Str description;
		user_op_describe_read(op, &name, &description);

		auto if_exists = ddl_if_exists(flags);
		write = user_describe(self, tr, &name, &description, if_exists);
		break;
	}
	case USER_LIMIT_SET:
	{
		Str    name;
		Limits limits;
		limits_init(&limits);
		user_op_limit_set_read(op, &name, &limits);

		auto if_exists = ddl_if_exists(flags);
		write = user_limit_set(self, tr, &name, &limits, if_exists);
		break;
	}
	case USER_LIMIT_UNSET:
	{
		Str      name;
		uint64_t mask;
		user_op_limit_unset_read(op, &name, &mask);

		auto if_exists = ddl_if_exists(flags);
		write = user_limit_unset(self, tr, &name, mask, if_exists);
		break;
	}
	case USER_API_CREATE:
	{
		Str  name;
		auto api = user_op_api_create_read(op, &name);
		defer(api_free, api);

		auto if_exists = ddl_if_exists(flags);
		write = user_api_create(self, tr, &name, api, if_exists);
		break;
	}
	case USER_API_DROP:
	{
		Str name;
		Str uri;
		user_op_api_drop_read(op, &name, &uri);

		auto if_exists = ddl_if_exists(flags);
		write = user_api_drop(self, tr, &name, &uri, if_exists);
		break;
	}
	default:
		error("unrecognized user operation id: {d}", (int)cmd);
		break;
	}

	return write;
}
