
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
engine_init(Engine*     self,
            CompactMgr* compact_mgr,
            Uuid*       id,
            Schema*     schema,
            int         compression,
            bool        crc)
{
	self->id          = id;
	self->compression = compression;
	self->crc         = crc;
	self->schema      = schema;
	self->list_count  = 0;

	part_map_init(&self->map);
	list_init(&self->list);
	service_init(&self->service, compact_mgr, self);
}

void
engine_free(Engine* self)
{
	service_free(&self->service);
	part_map_free(&self->map);
}

void
engine_start(Engine* self, bool start_service)
{
	part_map_create(&self->map, ROW_PARTITION_MAX);
	engine_recover(self);
	if (start_service)
		service_start(&self->service);
}

void
engine_stop(Engine* self)
{
	service_stop(&self->service);

	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
engine_debug(Engine* self)
{
	int count_total = 0;
	int count_total_collisions = 0;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		int count = 0;
		int count_collisions = 0;
		for (int i = 0; i < part->heap.table_size; i++)
		{
			auto head = part->heap.table[i];
			if (head)
			{
				count++;
				if (head->prev)
					count_collisions++;
			}
		}

		log("part [%4d: %4d)  %6d  %6d",
		     part->range_start, part->range_end, count, count_collisions);

		count_total += count;
		count_total_collisions += count_collisions;
	}

	log("%6d %6d", count_total, count_total_collisions);
}
