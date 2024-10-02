
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
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
db_init(Db*        self,
        PartMapper mapper,
        void*      mapper_arg,
        NodeIf*    node_iface,
        void*      node_iface_arg)
{
	schema_mgr_init(&self->schema_mgr);
	table_mgr_init(&self->table_mgr, mapper, mapper_arg);
	view_mgr_init(&self->view_mgr);
	node_mgr_init(&self->node_mgr, node_iface, node_iface_arg);
	checkpoint_mgr_init(&self->checkpoint_mgr, &db_checkpoint_if, self);
	checkpointer_init(&self->checkpointer, &self->checkpoint_mgr);
	wal_init(&self->wal);
}

void
db_free(Db* self)
{
	table_mgr_free(&self->table_mgr);
	node_mgr_free(&self->node_mgr);
	view_mgr_free(&self->view_mgr);
	schema_mgr_free(&self->schema_mgr);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	wal_free(&self->wal);
}

static void
db_create_system_schema(Db* self, const char* schema, bool create)
{
	Tr tr;
	tr_init(&tr);
	guard(tr_free, &tr);

	Exception e;
	if (enter(&e))
	{
		// begin
		tr_begin(&tr);

		// create system schema
		Str name;
		str_init(&name);
		str_set_cstr(&name, schema);
		auto config = schema_config_allocate();
		guard(schema_config_free, config);
		schema_config_set_name(config, &name);
		schema_config_set_system(config, true);
		schema_config_set_create(config, create);
		schema_mgr_create(&self->schema_mgr, &tr, config, false);

		// commit
		tr_commit(&tr);
	}

	if (leave(&e))
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
db_open(Db* self)
{
	// prepare system schemas
	db_create_system_schema(self, "system", false);
	db_create_system_schema(self, "public", true);

	// read directory and restore last checkpoint catalog
	// (schemas, tables, views)
	checkpoint_mgr_open(&self->checkpoint_mgr);
}

void
db_close(Db* self)
{
	// stop checkpointer service
	checkpointer_stop(&self->checkpointer);

	// free views
	view_mgr_free(&self->view_mgr);

	// free tables
	table_mgr_free(&self->table_mgr);

	// free nodes
	node_mgr_free(&self->node_mgr);

	// todo: wal close?
}
