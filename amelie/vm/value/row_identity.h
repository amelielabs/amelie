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

hot static inline void
row_identity(Column*  column, Value* refs,
             Value*   row,
             Value*   identity,
             Random*  random,
             uint64_t time_ms)
{
	// get existing identity value
	auto value = row + column->order;
	if (value->type == TYPE_REF)
		value = &refs[value->integer];
	if (value->type != TYPE_NULL)
		return;

	// generate
	auto seed = random_generate(random);
	if (column->type == TYPE_INT)
	{
		seed %= column->constraints.identity_modulo;
		value_set_int(identity, seed);
	} else
	{
		// uuid
		identity->type = TYPE_UUID;
		uuid_generate_as(&identity->uuid, seed, time_ms);
	}
}
