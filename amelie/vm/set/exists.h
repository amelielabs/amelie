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

static inline void
value_exists(Value* result, Value* value)
{
	if (unlikely(value->type == TYPE_NULL)) {
		value_set_bool(result, false);
		return;
	}
	assert(value->type == TYPE_STORE);
	auto it = store_iterator(value->store);
	defer(store_iterator_close, it);
	value_set_bool(result, store_iterator_has(it));
}
