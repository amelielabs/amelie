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

typedef struct ManifestRel ManifestRel;
typedef struct Manifest    Manifest;

struct ManifestRel
{
	Str     user;
	Str     name;
	int64_t column;
	int     pages_count;
	Buf     pages;
};

struct Manifest
{
	int     list_count;
	Buf     list;
	int64_t psn;
};

void manifest_init(Manifest*);
void manifest_free(Manifest*);
void manifest_open(Manifest*, char*);
ManifestRel*
manifest_find(Manifest*, Str*, Str*, int);
