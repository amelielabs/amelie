#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Engine Engine;

struct Engine
{
	PartMap map;
	List    list;
	int     list_count;
	Service service;
	int     compression;
	bool    crc;
	Uuid*   id;
	Schema* schema;
};

void engine_init(Engine*, CompactMgr*, Uuid*, Schema*, int, bool);
void engine_free(Engine*);
void engine_start(Engine*, bool);
void engine_stop(Engine*);
void engine_debug(Engine*);
