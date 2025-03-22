
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
#include <amelie_heap.h>
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
#include <amelie_frontend.h>
#include <amelie_backend.h>

static inline void
backend_bootstrap_workers(Db* db, Tr* tr, int workers)
{
	// precreate backend workers
	while (workers-- > 0)
	{
		auto config = worker_config_allocate();
		defer(worker_config_free, config);

		// set worker id
		Uuid id;
		uuid_generate(&id, global()->random);

		char uuid[UUID_SZ];
		uuid_get(&id, uuid, sizeof(uuid));

		Str uuid_str;
		str_set_cstr(&uuid_str, uuid);
		worker_config_set_id(config, &uuid_str);

		// create worker
		worker_mgr_create(&db->worker_mgr, tr, config, false);
	}
}

void
backend_bootstrap(Db* db, int workers)
{
	if (! workers)
		return;

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		// precreate backend workers
		backend_bootstrap_workers(db, &tr, workers);

		// commit
		tr_commit(&tr);
	);

	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}
