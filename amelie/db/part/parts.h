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

typedef struct PartsIf PartsIf;
typedef struct Parts   Parts;

struct PartsIf
{
	void (*attach)(Part*);
	void (*detach)(Part*);
	void (*sync)(Part*);
	void (*truncate)(Part*);
};

struct Parts
{
	Part*    part;
	PartArg* arg;
	PartsIf* iface;
	void*    iface_arg;
};

void parts_init(Parts*, PartsIf*, void*, PartArg*);
void parts_free(Parts*);
void parts_open(Parts*, List*);
void parts_close(Parts*);
void parts_truncate(Parts*);
void parts_index_create(Parts*, IndexConfig*);
void parts_index_remove(Parts*, Str*);
void parts_column_create(Parts*, Column*, uint32_t);
