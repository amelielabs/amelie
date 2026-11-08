
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
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_drop_of(Catalog* self, Tr* tr, Rel* rel)
{
	// (no checks)
	rels_drop(&self->rels, tr, rel);
}

bool
catalog_drop(Catalog* self, Tr* tr, RelType type, Str* name,
             bool     if_exists,
             bool     cascade)
{
	auto rel = catalog_find(self, REL_UNDEF, name, false);
	if (! rel)
	{
		if (! if_exists)
			error("{s} '{str}': not exists", rel_type_of(type), name);
		return false;
	}

	if (rel->type != type)
		error("relation '{str}': is not a {s}", name, rel_type_of(type));

	// collect dependencies
	Buf deps;
	buf_init(&deps);
	defer_buf(&deps);
	auto count = catalog_deps(self, rel, &deps);
	if (count > 0)
	{
		// cascade drop
		if (! cascade)
		{
			auto first = *(Rel**)deps.start;
			if (count >= 2)
				error("{s} '{str}': depends on {s} '{str}' and {d} more",
				      rel_type_of(type), name,
				      rel_type_of(first->type), first->name,
				      count - 1);
			else
				error("{s} '{str}': depends on {s} '{str}'",
				      rel_type_of(type), name,
				      rel_type_of(first->type), first->name);
		}
		catalog_deps_drop(self, tr, &deps);
	}

	// self
	catalog_drop_of(self, tr, rel);
	return true;
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str name;
	unpack_str(&pos, &name);
	Catalog* catalog = op->iface_arg;
	rels_rename(&catalog->rels, op->rel, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
catalog_rename_of(Catalog* self,
                  Tr*      tr,
                  Rel*     rel,
                  Str*     name_new)
{
	// (no checks)

	// update table
	log_ddl(&tr->log, &rename_if, self, rel);

	// save previous name
	encode_str(&tr->log.data, rel->name);

	// set new name
	rels_rename(&self->rels, rel, name_new);
}

bool
catalog_rename(Catalog* self,
               Tr*      tr,
               RelType  type,
               Str*     name,
               Str*     name_new,
               bool     if_exists)
{
	auto rel = catalog_find(self, REL_UNDEF, name, false);
	if (! rel)
	{
		if (! if_exists)
			error("{s} '{str}': not exists", rel_type_of(type), name);
		return false;
	}

	if (rel->type != type)
		error("relation '{str}': is not a {s}", name, rel_type_of(type));

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, name_new, false))
		error("relation '{str}': already exists", name_new);

	// ensure no strict dependecies
	catalog_deps_validate(self, rel, true);

	catalog_rename_of(self, tr, rel, name_new);
	return true;
}

static void
describe_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
describe_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str description;
	unpack_str(&pos, &description);
	str_free(op->rel->description);
	str_copy(op->rel->description, &description);
}

static LogIf describe_if =
{
	.commit = describe_if_commit,
	.abort  = describe_if_abort
};

void
catalog_describe_of(Catalog* self,
                    Tr*      tr,
                    Rel*     rel,
                    Str*     description)
{
	// (no check)
	unused(self);
	assert(rel->description);

	// update table
	log_ddl(&tr->log, &describe_if, NULL, rel);

	// save previous description
	encode_str(&tr->log.data, rel->description);

	// set new description
	str_free(rel->description);
	str_copy(rel->description, description);
}

bool
catalog_describe(Catalog* self,
                 Tr*      tr,
                 RelType  type,
                 Str*     name,
                 Str*     description,
                 bool     if_exists)
{
	auto rel = catalog_find(self, REL_UNDEF, name, false);
	if (! rel)
	{
		if (! if_exists)
			error("{s} '{str}': not exists", rel_type_of(type), name);
		return false;
	}

	if (rel->type != type)
		error("relation '{str}': is not a {s}", name, rel_type_of(type));

	if (! rel->description)
		error("{s} '{str}': does not support description",
		      rel_type_of(type), name);

	catalog_describe_of(self, tr, rel, description);
	return true;
}
