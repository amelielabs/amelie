
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>

hot static inline bool
group_node_cmp(HashtableNode* node, void* ptr)
{
	Group*     self = ((void**)ptr)[0];
	GroupNode* key  = ((void**)ptr)[1];
	auto ref = container_of(node, GroupNode, node);
	for (int i = 0; i < self->keys_count; i++)
	{
		auto a = &ref->keys[i];
		auto b = &key->keys[i];
		if (! value_is_equal(a, b))
			return false;
	}
	return true;
}

hot static inline GroupNode*
group_node_find(Group* self, GroupNode* key)
{
	HashtableNode* node;
	if (self->keys_count == 0) {
		node = hashtable_at(&self->ht, 0); // 0
	} else {
		void* argv[] = { self, key };
		node = hashtable_get(&self->ht, key->node.hash, group_node_cmp, argv);
	}
	if (node)
		return container_of(node, GroupNode, node);
	return NULL;
}

static void
group_node_free(void** argv)
{
	Group*     self = ((void**)argv)[0];
	GroupNode* node = ((void**)argv)[1];
	for (int j = 0; j < self->keys_count; j++)
		value_free(&node->keys[j]);
	in_free(node);
}

hot static inline void
group_merge_node(Group* self, GroupNode* node)
{
	void* argv[] = { self, node };
	guard(node_guard, group_node_free, argv);

	// move node if not exists
	auto src = group_node_find(self, node);
	if (src == NULL)
	{
		// resize hash table if necessary
		hashtable_reserve(&self->ht);

		// move node
		hashtable_set(&self->ht, &node->node);
		unguard(&node_guard);
		return;
	}

	// merge aggregate states
	uint8_t* state_a = (uint8_t*)src  + node->aggr_offset;
	uint8_t* state_b = (uint8_t*)node + node->aggr_offset;
	list_foreach(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_merge(aggr, state_a, state_b);
		int state_size = aggr_state_size(aggr);
		state_a += state_size;
		state_b += state_size;
	}

	// free node
}

hot void
group_merge(Value** list, int list_count)
{
	if (list_count <= 1)
		return;

	auto self = (Group*)list[0]->obj;
	for (int i = 1; i < list_count; i++)
	{
		auto group = (Group*)list[i]->obj;
		if (group->ht.count == 0)
			continue;

		int pos = -1;
		for (;;)
		{
			pos = group_next(group, pos);
			if (pos == -1)
				break;

			auto node = group_at(group, pos);
			hashtable_delete(&group->ht, &node->node);

			group_merge_node(self, node);
		}
	}
}
