
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_part.h>

static void
part_catalog_dump(Catalog* cat, Buf* buf)
{
	PartMgr* self = cat->iface_arg;
	part_mgr_dump(self, buf);
}

static inline void
part_catalog_restore_part(PartMgr* self, uint64_t lsn, uint8_t** pos)
{
	// [config, indexes[]]
	int count;
	data_read_array(pos, &count);

	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// start transaction
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// read part config
		auto config = part_config_read(pos);
		guard(config_guard, part_config_free, config);

		// create partition
		auto part = part_mgr_create(self, &trx, config);

		// create indexes
		data_read_array(pos, &count);
		for (int i = 0; i < count; i++)
		{
			auto index_config = index_config_read(pos);
			guard(config_index_guard, index_config_free, index_config);

			// todo: chose by type
			auto index = tree_allocate(index_config, &part->config->id);
			storage_attach(&part->storage, index);
		}
	}

	if (catch(&e))
	{
		transaction_abort(&trx);
		rethrow();
	}

	// set lsn and commit
	transaction_set_lsn(&trx, lsn);
	transaction_commit(&trx);
}

static void
part_catalog_restore(Catalog* cat, uint64_t lsn, uint8_t** pos)
{
	PartMgr* self = cat->iface_arg;

	// [partitions]
	int count;
	data_read_array(pos, &count);
	int i = 0;
	for (; i < count; i++)
		part_catalog_restore_part(self, lsn, pos);
}

CatalogIf part_catalog_if =
{
	.dump    = part_catalog_dump,
	.restore = part_catalog_restore
};
