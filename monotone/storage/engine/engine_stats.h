#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct EngineStats EngineStats;

struct EngineStats
{
	uint64_t count;
	uint64_t count_part;
	uint64_t count_part_pending;
	uint64_t size;
};

void engine_stats_get(Engine*, EngineStats*);
