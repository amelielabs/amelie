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

typedef struct Resource Resource;

struct Resource
{
	Portal* portal;
	Json    json;
};

void resource_init(Resource*, Portal*);
void resource_free(Resource*);
void resource_reset(Resource*);
bool resource_parse(Resource*, Str*, Request*);
