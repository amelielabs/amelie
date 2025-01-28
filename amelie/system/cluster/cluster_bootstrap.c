
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
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
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_cluster.h>

static inline void
cluster_bootstrap_nodes(Db* db, Tr* tr, int backends)
{
	// precreate compute nodes
	while (backends-- > 0)
	{
		auto config = node_config_allocate();
		defer(node_config_free, config);

		// set node id
		Uuid id;
		uuid_generate(&id, global()->random);

		char uuid[UUID_SZ];
		uuid_get(&id, uuid, sizeof(uuid));

		Str uuid_str;
		str_set_cstr(&uuid_str, uuid);
		node_config_set_id(config, &uuid_str);

		// create node
		node_mgr_create(&db->node_mgr, tr, config, false);
	}
}

void
cluster_bootstrap(Db* db, int backends)
{
	if (! backends)
		return;

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		// precreate compute nodes
		cluster_bootstrap_nodes(db, &tr, backends);

		// commit
		tr_commit(&tr);
	);

	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}
