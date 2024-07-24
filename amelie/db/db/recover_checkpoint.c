
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>

hot static void
recover_partition(Part* self, Table* table)
{
	auto checkpoint = config_checkpoint();
	log("recover %" PRIu64 ": %.*s.%.*s (partition %" PRIu64 ")",
	    checkpoint,
	    str_size(&table->config->schema),
	    str_of(&table->config->schema),
	    str_size(&table->config->name),
	    str_of(&table->config->name),
	    self->config->id);

	SnapshotCursor cursor;
	snapshot_cursor_init(&cursor);
	guard(snapshot_cursor_close, &cursor);

	snapshot_cursor_open(&cursor, checkpoint, self->config->id);
	uint64_t count = 0;
	for (;;)
	{
		auto buf = snapshot_cursor_next(&cursor);
		if (! buf)
			break;
		guard_buf(buf);
		auto pos = msg_of(buf)->data;
		part_ingest(self, &pos);
		count++;
	}

	log("recover %" PRIu64 ": %.*s.%.*s (partition %" PRIu64 ") %" PRIu64 " rows loaded",
	    checkpoint,
	    str_size(&table->config->schema),
	    str_of(&table->config->schema),
	    str_size(&table->config->name),
	    str_of(&table->config->name),
	    self->config->id,
	    count);
}

hot void
recover_checkpoint(Db* self, Uuid* node)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			if (! uuid_compare(&part->config->node, node))
				continue;
			recover_partition(part, table);
		}
	}
}
