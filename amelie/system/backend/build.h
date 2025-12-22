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

typedef struct BuildConfig BuildConfig;
typedef struct Build       Build;

typedef enum
{
	BUILD_NONE,
	BUILD_RECOVER,
	BUILD_COLUMN_ADD,
	BUILD_COLUMN_DROP,
	BUILD_INDEX
} BuildType;

struct BuildConfig
{
	BuildType    type;
	Table*       table;
	Table*       table_new;
	Column*      column;
	IndexConfig* index;
};

struct Build
{
	BuildConfig* config;
	Dispatch*    dispatch;
	Dtr          dtr;
	Program*     program;
	Local        local;
};

void build_init(Build*);
void build_free(Build*);
void build_reset(Build*);
void build_prepare(Build*, BuildConfig*);
void build_add(Build*, Part*);
void build_add_all(Build*, Storage*);
void build_run(Build*);
void build_execute(Build*, Part*);
