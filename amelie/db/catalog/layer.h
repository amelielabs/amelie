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

typedef struct Layer Layer;

struct Layer
{
	Rel          rel;
	LayerConfig* config;
	Table*       table;
};

bool layer_create(Catalog*, Tr*, LayerConfig*, bool);

always_inline static inline Layer*
layer_of(Rel* self)
{
	return (Layer*)self;
}
