
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
publish_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
publish_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf publish_if =
{
	.commit = publish_if_commit,
	.abort  = publish_if_abort
};

void
publish(Topic* self, Tr* tr, uint8_t* data, int data_size)
{
	// ensure permission to publish
	check_permission(tr, &self->rel, PERM_PUBLISH);

	/*if (topic->cdc)*/

	// publish command
	log_cmd(&tr->log, CMD_PUBLISH, &publish_if, NULL, &self->rel);
	log_persist_cmd(&tr->log, &self->config->id, data);

	write_cdc_add(&tr->log.write_cdc, CMD_PUBLISH, &self->config->id,
	              data, data_size);
}
