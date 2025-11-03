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

typedef enum
{
	ENDPOINT_CSV,
	ENDPOINT_JSONL,
	ENDPOINT_JSON
} EndpointType;

struct Endpoint
{
	EndpointType type;
	Columns*     columns;
	Ast*         columns_list;
	int          columns_list_count;
	bool         columns_list_has;
	Set*         values;
	Udf*         udf;
	Table*       table;
	Uri*         uri;
};

static inline void
endpoint_init(Endpoint* self, Uri* uri, EndpointType type)
{
	self->type               = type;
	self->columns            = NULL;
	self->columns_list       = NULL;
	self->columns_list_count = 0;
	self->columns_list_has   = false;
	self->udf                = NULL;
	self->table              = NULL;
	self->uri                = uri;
}
