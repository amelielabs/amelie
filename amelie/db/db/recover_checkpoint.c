
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

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
