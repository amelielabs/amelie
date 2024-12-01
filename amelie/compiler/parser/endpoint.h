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

typedef struct Endpoint Endpoint;

struct Endpoint
{
	Ast*   columns;
	int    columns_count;
	bool   columns_has;
	Table* table;
	Uri*   uri;
};

static inline void
endpoint_init(Endpoint* self, Uri* uri)
{
	self->columns       = NULL;
	self->columns_count = 0;
	self->columns_has   = false;
	self->table         = false;
	self->uri           = uri;
}
