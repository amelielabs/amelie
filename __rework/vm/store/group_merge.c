
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
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

hot static inline bool
group_node_cmp(HashtableNode* node, void* ptr)
{
	Group*     self = ((void**)ptr)[0];
	GroupNode* key  = ((void**)ptr)[1];
	auto ref = container_of(node, GroupNode, node);
	for (int i = 0; i < self->keys_count; i++)
	{
		auto a = &ref->value[i];
		auto b = &key->value[i];
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
		node = hashtable_at(&self->ht, 0);
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
	for (int i = 0; i < self->keys_count + self->aggs_count; i++)
		value_free(&node->value[i]);
	am_free(node);
}

hot static inline void
group_merge_node(Group* self, GroupNode* node)
{
	void* argv[] = { self, node };
	guard(group_node_free, argv);

	// move node if not exists
	auto src = group_node_find(self, node);
	if (src == NULL)
	{
		// resize hash table if necessary
		hashtable_reserve(&self->ht);

		// move node
		hashtable_set(&self->ht, &node->node);
		unguard();
		return;
	}

	// merge aggregate states
	for (auto i = 0; i < self->aggs_count; i++)
	{
		auto agg     = group_agg(self, i);
		auto state_a = &src->value[self->keys_count + i];
		auto state_b = &node->value[self->keys_count + i];
		if (unlikely(agg->type == GROUP_LAMBDA))
			error("operation not supported");

		// create and set new aggregation state
		// a = a x b
		Agg state;
		agg_init(&state);
		value_agg(&state, state_a, agg->type_agg, state_b);
		value_set_agg(state_a, &state);
	}

	// free node
}

hot void
group_merge(Value** list, int list_count)
{
	if (list_count <= 1)
		return;

	auto self = (Group*)list[0]->store;
	for (int i = 1; i < list_count; i++)
	{
		auto group = (Group*)list[i]->store;
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
