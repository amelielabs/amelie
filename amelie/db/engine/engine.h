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

typedef struct EngineIf EngineIf;
typedef struct Engine   Engine;

struct EngineIf
{
	void (*attach)(Engine*, Level*);
	void (*detach)(Engine*, Level*);
};

struct Engine
{
	PartMapping mapping;
	Level*      levels;
	int         levels_count;
	Uuid*       id_table;
	Sequence*   seq;
	bool        unlogged;
	EngineIf*   iface;
	void*       iface_arg;
	StorageMgr* storage_mgr;
};

void  engine_init(Engine*, EngineIf*, void*, StorageMgr*, Uuid*, Sequence*, bool, Keys*);
void  engine_free(Engine*);
void  engine_open(Engine*, List*, List*, int);
void  engine_close(Engine*);
Part* engine_find(Engine*, uint64_t);
Buf*  engine_status(Engine*, Str*, bool);
Iterator*
engine_iterator(Engine*, Part*, IndexConfig*, bool, Row*);
