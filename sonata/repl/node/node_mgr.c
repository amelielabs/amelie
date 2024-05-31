
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
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
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
#include <sonata_node.h>

void
node_mgr_init(NodeMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
node_mgr_free(NodeMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto node = list_at(Node, link);
		node_free(node);
	}
}

static inline void
node_mgr_save(NodeMgr* self)
{
	// create dump
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);

	// update and save state
	var_data_set_buf(&config()->nodes, buf);
	buf_end(buf);

	control_save_config();
}

void
node_mgr_open(NodeMgr* self)
{
	auto nodes = &config()->nodes;
	if (! var_data_is_set(nodes))
		return;
	auto pos = var_data_of(nodes);
	if (data_is_null(pos))
		return;

	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		auto config = node_config_read(&pos);
		guard(node_config_free, config);

		auto node = node_allocate(config);
		list_append(&self->list, &node->link);
		self->list_count++;
	}
}

void
node_mgr_create(NodeMgr* self, NodeConfig* config, bool if_not_exists)
{
	auto node = node_mgr_find(self, &config->name);
	if (node)
	{
		if (! if_not_exists)
			error("node '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}
	node = node_allocate(config);
	list_append(&self->list, &node->link);
	self->list_count++;
	node_mgr_save(self);
}

void
node_mgr_drop(NodeMgr* self, Str* name, bool if_exists)
{
	auto node = node_mgr_find(self, name);
	if (! node)
	{
		if (! if_exists)
			error("node '%.*s': not exists", str_size(name), str_of(name));
		return;
	}
	list_unlink(&node->link);
	self->list_count--;
	node_mgr_save(self);
}

Buf*
node_mgr_list(NodeMgr* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}

Node*
node_mgr_find(NodeMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		if (str_compare(&node->config->name, name))
			return node;
	}
	return NULL;
}

Node*
node_mgr_find_by_id(NodeMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		if (uuid_compare(&node->config->id, id))
			return node;
	}
	return NULL;
}
