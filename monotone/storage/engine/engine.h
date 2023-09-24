#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Engine       Engine;
typedef struct EngineConfig EngineConfig;

struct EngineConfig
{
	Uuid*   id;
	int     range_start;
	int     range_end;
	int     compression;
	bool    crc;
	Schema* schema;
};

struct Engine
{
	PartMap       map;
	List          list;
	int           list_count;
	Service       service;
	EngineConfig* config;
};

void engine_init(Engine*, EngineConfig*, CompactMgr*);
void engine_free(Engine*);
void engine_start(Engine*, bool);
void engine_stop(Engine*);
void engine_debug(Engine*);
