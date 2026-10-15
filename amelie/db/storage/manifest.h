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

typedef struct ManifestPart ManifestPart;
typedef struct ManifestRel  ManifestRel;
typedef struct Manifest     Manifest;

struct ManifestPart
{
	int     order;
	int     pages_count;
	int64_t pages[];
};

struct ManifestRel
{
	Str     user;
	Str     name;
	int64_t column;
	int     parts_count;
	Buf     parts;
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
ManifestPart*
manifest_find(Manifest*, Str*, Str*, int, int);
