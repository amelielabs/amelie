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
value_agg(Agg* state, Value* value_state, int type, Value* value)
{
	// get or create state
	if (value_state->type == VALUE_AGG)
		*state = value_state->agg;
	else
	if (value_state->type == VALUE_NULL)
		agg_init(state);
	else
		error("unsupported aggregate state");

	if (value->type == VALUE_AGG)
	{
		// combine agg states
		agg_merge(state, type, &value->agg);
	} else
	{
		// process next value
		AggValue aggval;
		int      aggval_type;
		switch (value->type) {
		case VALUE_NULL:
			aggval_type = AGG_NULL;
			break;
		case VALUE_INT:
			aggval_type = AGG_INT;
			aggval.integer = value->integer;
			break;
		case VALUE_DOUBLE:
			aggval_type = AGG_REAL;
			aggval.real = value->dbl;
			break;
		default:
		{
			if (type == AGG_COUNT)
			{
				aggval_type = AGG_INT;
				aggval.integer = value->integer;
				break;
			}
			error("unsupported aggregate operation type");
		}
		}
		agg_step(state, type, aggval_type, &aggval);
	}
}
