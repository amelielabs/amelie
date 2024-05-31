#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Node Node;

struct Node
{
	NodeConfig* config;
	List        link;
};

static inline void
node_free(Node* node)
{
	if (node->config)
		node_config_free(node->config);
	so_free(node);
}

static inline Node*
node_allocate(NodeConfig* config)
{
	Node* self;
	self = so_malloc(sizeof(*self));
	self->config = NULL;
	list_init(&self->link);
	guard(node_free, self);
	self->config = node_config_copy(config);
	return unguard();
}
