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

static inline bool
parse_if_not_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	stmt_expect(self, KNOT);
	stmt_expect(self, KEXISTS);
	return true;
}

static inline bool
parse_if_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	stmt_expect(self, KEXISTS);
	return true;
}

static inline bool
parse_target_path(Ast* path, Str* schema, Str* name)
{
	str_init(schema);
	str_init(name);

	// name
	if (path->id == KNAME)
	{
		str_set(schema, "public", 6);
		str_set_str(name, &path->string);
		return true;
	}

	// schema.name
	if (path->id == KNAME_COMPOUND)
	{
		str_split(&path->string, schema, '.');
		str_set_str(name, &path->string);
		str_advance(name, str_size(schema) + 1);
		if (str_chr(name, '.'))
			return false;
		return true;
	}

	return false;
}

static inline Ast*
parse_target(Stmt* self, Str* schema, Str* name)
{
	auto path = stmt_next(self);
	if (! parse_target_path(path, schema, name))
	{
		stmt_push(self, path);
		return NULL;
	}
	return path;
}

static inline void
parse_set_target_column(Str* self, Str* target, Str* column)
{
	auto size = str_size(target) + 1 + str_size(column);
	auto name = palloc(size);
	memcpy(name, target->pos, str_size(target));
	memcpy(name + str_size(target), ".", 1);
	memcpy(name + str_size(target) + 1, column->pos, str_size(column));
	str_set(self, name, size);
}
