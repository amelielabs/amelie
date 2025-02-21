
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
worker_mgr_init(WorkerMgr* self, WorkerIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	handle_mgr_init(&self->mgr);
}

void
worker_mgr_free(WorkerMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
worker_mgr_create(WorkerMgr*    self,
                  Tr*           tr,
                  WorkerConfig* config,
                  bool          if_not_exists)
{
	// make sure worker does not exists
	auto current = worker_mgr_find(self, &config->id, false);
	if (current)
	{
		if (! if_not_exists)
			error("worker '%.*s': already exists", str_size(&config->id),
			      str_of(&config->id));
		return;
	}

	Uuid id;
	uuid_init(&id);
	uuid_set(&id, &config->id);

	// allocate worker and init
	auto worker = worker_allocate(config, &id, self->iface, self->iface_arg);

	// save create worker operation
	auto op = worker_op_create(config);

	// update workers
	handle_mgr_create(&self->mgr, tr, CMD_WORKER_CREATE, &worker->handle, op);

	// create worker context
	worker_create(worker);
}

void
worker_mgr_drop(WorkerMgr* self,
                Tr*        tr,
                Str*       id,
                bool       if_exists)
{
	auto worker = worker_mgr_find(self, id, false);
	if (! worker)
	{
		if (! if_exists)
			error("worker '%.*s': not exists", str_size(id),
			      str_of(id));
		return;
	}

	// ensure worker has no dependencies
	if (worker->route.refs > 0)
		error("worker '%.*s': has dependencies", str_size(id),
		      str_of(id));

	// save drop worker operation
	auto op = worker_op_drop(&worker->config->id);

	// update mgr
	handle_mgr_drop(&self->mgr, tr, CMD_WORKER_DROP, &worker->handle, op);
}

void
worker_mgr_dump(WorkerMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto worker = worker_of(list_at(Handle, link));
		worker_config_write(worker->config, buf);
	}
	encode_array_end(buf);
}

Buf*
worker_mgr_list(WorkerMgr* self, Str* id)
{
	auto buf = buf_create();
	if (id)
	{
		auto worker = worker_mgr_find(self, id, false);
		if (worker)
			worker_config_write(worker->config, buf);
		else
			encode_null(buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto worker = worker_of(list_at(Handle, link));
			worker_config_write(worker->config, buf);
		}
		encode_array_end(buf);
	}
	return buf;
}

Worker*
worker_mgr_find(WorkerMgr* self, Str* id, bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, NULL, id);
	if (! handle)
	{
		if (error_if_not_exists)
			error("worker '%.*s': not exists", str_size(id),
			      str_of(id));
		return NULL;
	}
	return worker_of(handle);
}
