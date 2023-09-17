
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>

void
engine_stats_get(Engine* self, EngineStats* stats)
{
	stats->count              = 0;
	stats->count_part         = self->list_count;
	stats->count_part_pending = 0;
	stats->size               = 0;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		stats->count              += part->heap.count;
		stats->size               += 0; // todo
		stats->count_part_pending += heap_is_full(&part->heap);
	}
}
