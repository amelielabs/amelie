
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
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>

static void
build_index(Recover* self, Table* table, IndexConfig* index)
{
	// do parallel indexation per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_INDEX, cluster, table, NULL, index);
	guard(build_free, &build);
	build_run(&build);
}

static void
build_table(Recover* self, Table* table, Table* table_dest)
{
	// do parallel table rebuild per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_TABLE, cluster, table, table_dest, NULL);
	guard(build_free, &build);
	build_run(&build);
}

RecoverIf build_if =
{
	.indexate = build_index,
	.build    = build_table
};
