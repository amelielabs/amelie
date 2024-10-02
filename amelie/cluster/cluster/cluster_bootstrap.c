
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>

void
cluster_bootstrap(Db* db, int backends)
{
	if (! backends)
		return;

	Tr tr;
	tr_init(&tr);

	WalBatch wal_batch;
	wal_batch_init(&wal_batch);

	Exception e;
	if (enter(&e))
	{
		// begin
		tr_begin(&tr);

		// precreate compute nodes
		while (backends-- > 0)
		{
			auto config = node_config_allocate();
			guard(node_config_free, config);

			// set node id
			Uuid id;
			uuid_generate(&id, global()->random);

			char uuid[UUID_SZ];
			uuid_to_string(&id, uuid, sizeof(uuid));

			Str uuid_str;
			str_set_cstr(&uuid_str, uuid);
			node_config_set_name(config, &uuid_str);

			// create node
			node_mgr_create(&db->node_mgr, &tr, config, false);
		}

		wal_batch_begin(&wal_batch, WAL_UTILITY);
		wal_batch_add(&wal_batch, &tr.log.log_set);
		wal_write(&db->wal, &wal_batch);

		// commit
		tr_commit(&tr);
		tr_free(&tr);
		wal_batch_free(&wal_batch);
	}

	if (leave(&e))
	{
		tr_abort(&tr);
		tr_free(&tr);
		wal_batch_free(&wal_batch);
		rethrow();
	}
}
