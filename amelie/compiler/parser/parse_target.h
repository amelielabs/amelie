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
