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

hot static inline int64_t
row_identity(Column* column, Value* refs,
             Value*  row,
             Value*  identity,
             Local*  local)
{
	// get existing identity value
	auto value = row + column->order;
	if (value->type == TYPE_REF)
		value = &refs[value->integer];
	if (value->type != TYPE_NULL)
	{
		*identity = *value;
		return identity->integer;
	}

	// generate
	auto cons = &column->constraints;
	uint64_t id;
	id = random_generate(&local->random) % cons->identity_modulo;
	value_set_int(identity, id);
	return id;
}
