#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Build Build;

typedef enum
{
	BUILD_NONE,
	BUILD_RECOVER,
	BUILD_COLUMN_ADD,
	BUILD_COLUMN_DROP,
	BUILD_INDEX
} BuildType;

struct Build
{
	BuildType    type;
	Table*       table;
	Table*       table_new;
	Column*      column;
	IndexConfig* index;
	Cluster*     cluster;
	Channel      channel;
};

void build_init(Build*, BuildType, Cluster*, Table*, Table*,
                Column*, IndexConfig*);
void build_free(Build*);
void build_run(Build*);
void build_execute(Build*, Uuid*);

extern RecoverIf build_if;
