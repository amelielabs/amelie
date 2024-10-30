
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

hot static inline void
group_free_node(Group* self, GroupNode* node)
{
	for (int i = 0; i < self->keys_count + self->aggs_count; i++)
		value_free(&node->value[i]);
	am_free(node);
}

static void
group_free(Store* store)
{
	auto self = (Group*)store;
	if (self->ht.count > 0)
	{
		auto index = (GroupNode**)(self->ht.buf.start);
		for (int i = 0; i < self->ht.size; i++)
		{
			auto node = index[i];
			if (node == NULL)
				continue;
			group_free_node(self, node);
		}
	}

	for (int i = 0; i < self->aggs_count; i++)
	{
		auto agg = group_agg(self, i);
		value_free(&agg->value);
	}
	buf_free(&self->aggs);
	hashtable_free(&self->ht);
	am_free(self);
}

Group*
group_create(int keys_count)
{
	Group* self = am_malloc(sizeof(Group));
	self->store.free   = group_free;
	self->store.encode = NULL;
	self->store.decode = NULL;
	self->store.in     = NULL;
	self->aggs_count   = 0;
	self->keys_count   = keys_count;
	buf_init(&self->aggs);
	hashtable_init(&self->ht);
	hashtable_create(&self->ht, 256);
	return self;
}

void
group_add(Group* self, int type, Value* init)
{
	auto agg = (GroupAgg*)buf_claim(&self->aggs, sizeof(GroupAgg));
	agg->type = type;
	agg->type_agg = AGG_UNDEF;
	switch (type) {
	case GROUP_COUNT:
		agg->type_agg = AGG_COUNT;
		break;
	case GROUP_MIN:
		agg->type_agg = AGG_MIN;
		break;
	case GROUP_MAX:
		agg->type_agg = AGG_MAX;
		break;
	case GROUP_SUM:
		agg->type_agg = AGG_SUM;
		break;
	case GROUP_AVG:
		agg->type_agg = AGG_AVG;
		break;
	}
	value_init(&agg->value);
	if (init)
		value_copy(&agg->value, init);
	self->aggs_count++;
}

typedef struct
{
	uint32_t hash;
	uint32_t size;
	Value**  target_data;
	Group*   self;
} GroupKey;

hot static inline bool
group_cmp(HashtableNode* node, void* ptr)
{
	GroupKey* key = ptr;
	auto ref = container_of(node, GroupNode, node);
	for (int i = 0; i < key->self->keys_count; i++)
	{
		auto a = &ref->value[i];
		auto b = key->target_data[i];
		if (! value_is_equal(a, b))
			return false;
	}
	return true;
}

hot static inline GroupNode*
group_create_node(GroupKey* key)
{
	auto self = key->self;

	// resize hash table if necessary
	hashtable_reserve(&self->ht);

	// [node][keys and aggs][variable_data]

	// allocate new node
	int index_size = sizeof(Value) * (self->keys_count + self->aggs_count);
	GroupNode* node;
	node = am_malloc(sizeof(GroupNode) + index_size + key->size);
	node->node.hash = key->hash;

	// copy keys
	auto pos = (uint8_t*)node + sizeof(GroupNode) + index_size;
	for (auto i = 0; i < self->keys_count; i++)
	{
		auto value = key->target_data[i];
		auto ref = &node->value[i];
		value_init(ref);
		switch (value->type) {
		case VALUE_INT:
			value_set_int(ref, value->integer);
			break;
		case VALUE_TIMESTAMP:
			value_set_timestamp(ref, value->integer);
			break;
		case VALUE_BOOL:
			value_set_bool(ref, value->integer);
			break;
		case VALUE_REAL:
			value_set_real(ref, value->real);
			break;
		case VALUE_STRING:
			memcpy(pos, str_of(&value->string), str_size(&value->string));
			value_set_string(ref, &value->string, NULL);
			pos += str_size(&value->string);
			break;
		case VALUE_OBJ:
			memcpy(pos, value->data, value->data_size);
			value_set_obj(ref, pos, value->data_size, NULL);
			pos += value->data_size;
			break;
		case VALUE_ARRAY:
			memcpy(pos, value->data, value->data_size);
			value_set_array(ref, pos, value->data_size, NULL);
			pos += value->data_size;
			break;
		default:
			assert(0);
			break;
		}
	}

	// create aggs
	for (auto i = 0; i < self->aggs_count; i++)
	{
		auto agg = group_agg(self, i);
		auto ref = &node->value[self->keys_count + i];
		value_init(ref);
		if (agg->type == GROUP_LAMBDA)
			value_copy(ref, &agg->value);
		else
			value_set_null(ref);
	}

	// add node to the hash table
	hashtable_set(&self->ht, &node->node);
	return node;
}

hot static inline GroupNode*
group_find(GroupKey* key)
{
	HashtableNode* node;
	if (key->self->keys_count == 0)
		node = hashtable_at(&key->self->ht, key->hash); // 0
	else
		node = hashtable_get(&key->self->ht, key->hash, group_cmp, key);
	if (likely(node))
		return container_of(node, GroupNode, node);
	return NULL;
}

hot static GroupNode*
group_find_or_create(Group* self, Value** target_data)
{
	GroupKey key =
	{
		.hash        = 0,
		.size        = 0,
		.target_data = target_data,
		.self        = self
	};

	// calculate size of variable keys
	for (int i = 0; i < self->keys_count; i++)
	{
		auto  value = target_data[i];
		int   data_size = 0;
		void* data = NULL;
		switch (value->type) {
		case VALUE_INT:
		case VALUE_BOOL:
		case VALUE_TIMESTAMP:
			data = &value->integer;
			data_size = sizeof(value->integer);
			break;
		case VALUE_REAL:
			data = &value->real;
			data_size = sizeof(value->real);
			break;
		case VALUE_STRING:
			data = str_of(&value->string);
			data_size = str_size(&value->string);
			key.size += data_size;
			break;
		case VALUE_OBJ:
		case VALUE_ARRAY:
			data = value->data;
			data_size = value->data_size;
			key.size += data_size;
			break;
		case VALUE_NULL:
		case VALUE_SET:
		case VALUE_GROUP:
		default:
			error("GROUP BY: unsupported key type");
			break;
		}
		key.hash ^= hash_fnv(data, data_size);
	}

	// find or create node
	auto node = group_find(&key);
	if (node)
		return node;
	return group_create_node(&key);
}

hot void
group_write(Group* self, Stack* stack)
{
	int count = self->keys_count + self->aggs_count;
	Value* data[count];
	for (int i = 0; i < count; i++)
		data[i] = stack_at(stack, count - i);

	Value** target_data = data;
	Value** target_data_aggs = data + self->keys_count;

	// find or create group node, copy group by keys
	auto node = group_find_or_create(self, target_data);

	// process aggs
	for (auto i = 0; i < self->aggs_count; i++)
	{
		auto agg = group_agg(self, i);
		auto ref = &node->value[self->keys_count + i];
		if (agg->type == GROUP_LAMBDA)
		{
			if (target_data_aggs[i]->type != VALUE_NULL)
			{
				value_free(ref);
				value_copy(ref, target_data_aggs[i]);
			}
		} else {
			// create and set new aggregation state
			Agg state;
			agg_init(&state);
			value_agg(&state, ref, agg->type_agg, target_data_aggs[i]);
			value_set_agg(ref, &state);
		}
	}
}

void
group_get(Group* self, Stack* stack, int pos, Value* value)
{
	int count = self->keys_count;
	Value* keys[count];
	for (int i = 0; i < count; i++)
		keys[i] = stack_at(stack, count - i);

	// find or create group node, copy group by keys
	auto node = group_find_or_create(self, keys);

	// read aggregate value
	value_copy(value, &node->value[self->keys_count + pos]);
}

void
group_read_aggr(Group* self, GroupNode* node, int pos, Value* value)
{
	auto agg = group_agg(self, pos);
	auto ref = &node->value[self->keys_count + pos];
	if (agg->type == GROUP_LAMBDA)
	{
		value_copy(value, ref);
	} else
	{
		// convert VALUE_AGG to VALUE_INT/VALUE_REAL
		if (ref->type == VALUE_AGG)
		{
			AggValue aggval;
			switch (agg_read(&ref->agg, &aggval)) {
			case AGG_INT:
				value_set_int(value, aggval.integer);
				break;
			case AGG_REAL:
				value_set_real(value, aggval.real);
				break;
			case AGG_NULL:
				if (agg->type == GROUP_COUNT)
					value_set_int(value, 0);
				else
					value_set_null(value);
				break;
			}
		} else {
			value_set_null(value);
		}
	}
}

void
group_read_keys(Group* self, GroupNode* node, Value* result)
{
	if (self->keys_count > 1)
	{
		auto buf = buf_create();
		encode_array(buf);
		for (int i = 0; i < self->keys_count; i++)
			value_write(&node->value[i], buf);
		encode_array_end(buf);
		value_set_array_buf(result, buf);
	} else {
		value_copy(result, &node->value[0]);
	}
}
