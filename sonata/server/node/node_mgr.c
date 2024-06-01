
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
	buf_free(buf);
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
	auto node = node_mgr_find(self, &config->id);
	if (node)
	{
		if (! if_not_exists)
		{
			char uuid[UUID_SZ];
			uuid_to_string(&config->id, uuid, sizeof(uuid));
			error("node '%s' already exists", uuid);
		}
		return;
	}
	node = node_allocate(config);
	list_append(&self->list, &node->link);
	self->list_count++;
	node_mgr_save(self);
}

void
node_mgr_drop(NodeMgr* self, Uuid* id, bool if_exists)
{
	auto node = node_mgr_find(self, id);
	if (! node)
	{
		if (! if_exists)
		{
			char uuid[UUID_SZ];
			uuid_to_string(id, uuid, sizeof(uuid));
			error("node '%s': not exists", uuid);
		}
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
node_mgr_find(NodeMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		if (uuid_compare(&node->config->id, id))
			return node;
	}
	return NULL;
}
