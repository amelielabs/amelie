#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline bool
parse_if_not_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	if (! stmt_if(self, KNOT))
		error("IF <NOT> EXISTS expected");
	if (! stmt_if(self, KEXISTS))
		error("IF NOT <EXISTS> expected");
	return true;
}

static inline bool
parse_if_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	if (! stmt_if(self, KEXISTS))
		error("IF <EXISTS> expected");
	return true;
}

static inline Ast*
parse_target(Stmt* self, Str* schema, Str* name)
{
	str_init(schema);
	str_init(name);

	// name
	auto path = stmt_next(self);
	if (path->id == KNAME)
	{
		str_set(schema, "public", 6);
		str_set_str(name, &path->string);
		return path;
	}

	// schema.name
	if (path->id == KNAME_COMPOUND)
	{
		str_split_or_set(&path->string, schema, '.');
		str_set_str(name, &path->string);
		str_advance(name, str_size(schema) + 1);
		if (strnchr(str_of(name), str_size(name), '.'))
		{
			stmt_push(self, path);
			return NULL;
		}
		return path;
	}

	stmt_push(self, path);
	return NULL;
}
