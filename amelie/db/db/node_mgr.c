
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
node_mgr_init(NodeMgr* self, NodeIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	handle_mgr_init(&self->mgr);
}

void
node_mgr_free(NodeMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
node_mgr_create(NodeMgr*    self,
                Tr*         tr,
                NodeConfig* config,
                bool        if_not_exists)
{
	// make sure node does not exists
	auto current = node_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("node '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	Uuid id;
	uuid_init(&id);
	uuid_from_string(&id, &config->name);

	// allocate node and init
	auto node = node_allocate(config, &id, self->iface, self->iface_arg);

	// save create node operation
	auto op = node_op_create(config);

	// update nodes
	handle_mgr_create(&self->mgr, tr, LOG_NODE_CREATE, &node->handle, op);

	// create node context
	node_create(node);
}

void
node_mgr_drop(NodeMgr* self,
              Tr*      tr,
              Str*     name,
              bool     if_exists)
{
	auto node = node_mgr_find(self, name, false);
	if (! node)
	{
		if (! if_exists)
			error("node '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// ensure node has no dependencies
	if (node->route.refs > 0)
		error("node '%.*s': has dependencies", str_size(name),
		      str_of(name));

	// save drop node operation
	auto op = node_op_drop(&node->config->name);

	// update mgr
	handle_mgr_drop(&self->mgr, tr, LOG_NODE_DROP, &node->handle, op);
}

void
node_mgr_dump(NodeMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto node = node_of(list_at(Handle, link));
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);
}

Buf*
node_mgr_list(NodeMgr* self)
{
	auto buf = buf_create();
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto node = node_of(list_at(Handle, link));
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);
	return buf;
}

Node*
node_mgr_find(NodeMgr* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, NULL, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("node '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return node_of(handle);
}
