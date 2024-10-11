
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
#include <amelie_value.h>
#include <amelie_aggr.h>

hot static inline void
group_free_node(Group* self, GroupNode* node)
{
	uint8_t* aggr_state = (uint8_t*)node + node->aggr_offset;
	list_foreach(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_state_free(aggr, aggr_state);
		aggr_state += aggr_state_size(aggr);
	}
	for (int j = 0; j < self->keys_count; j++)
		value_free(&node->keys[j]);
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

	list_foreach_safe(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_free(aggr);
	}
	list_init(&self->aggrs);

	self->aggr_size  = 0;
	self->aggr_count = 0;
	self->keys_count = 0;

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
	self->aggr_size    = 0;
	self->aggr_count   = 0;
	self->keys_count   = keys_count;
	list_init(&self->aggrs);
	hashtable_init(&self->ht);
	hashtable_create(&self->ht, 256);
	return self;
}

void
group_add(Group* self, AggrIf* iface, Value* init)
{
	auto aggr = aggr_create(iface, init);
	self->aggr_size += aggr_state_size(aggr);
	self->aggr_count++;
	list_append(&self->aggrs, &aggr->link);
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
		auto a = &ref->keys[i];
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

	// [node][values][variable_data][aggr_state]

	// allocate new node
	int index_size = sizeof(Value) * self->keys_count;
	GroupNode* node;
	node = am_malloc(sizeof(GroupNode) + index_size + key->size + self->aggr_size);
	node->node.hash = key->hash;

	// copy keys
	uint8_t* pos;
	pos = (uint8_t*)node + sizeof(GroupNode) + index_size;
	for (int i = 0; i < self->keys_count; i++)
	{
		auto value = key->target_data[i];
		value_init(&node->keys[i]);
		switch (value->type) {
		case VALUE_INT:
			value_set_int(&node->keys[i], value->integer);
			break;
		case VALUE_TIMESTAMP:
			value_set_timestamp(&node->keys[i], value->integer);
			break;
		case VALUE_BOOL:
			value_set_bool(&node->keys[i], value->integer);
			break;
		case VALUE_REAL:
			value_set_real(&node->keys[i], value->real);
			break;
		case VALUE_STRING:
			memcpy(pos, str_of(&value->string), str_size(&value->string));
			value_set_string(&node->keys[i], &value->string, NULL);
			pos += str_size(&value->string);
			break;
		case VALUE_OBJ:
			memcpy(pos, value->data, value->data_size);
			value_set_obj(&node->keys[i], pos, value->data_size, NULL);
			pos += value->data_size;
			break;
		case VALUE_ARRAY:
			memcpy(pos, value->data, value->data_size);
			value_set_array(&node->keys[i], pos, value->data_size, NULL);
			pos += value->data_size;
			break;
		default:
			assert(0);
			break;
		}
	}
	node->aggr_offset = pos - (uint8_t*)node;

	// create aggr states
	list_foreach(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_state_create(aggr, pos);
		pos += aggr_state_size(aggr);
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

hot static inline void
group_write_node(Group* self, GroupNode* node, Value** target_data_aggs)
{
	uint8_t* aggr_state = (uint8_t*)node + node->aggr_offset;
	int i = 0;
	list_foreach(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_write(aggr, aggr_state, target_data_aggs[i]);
		aggr_state += aggr_state_size(aggr);
		i++;
	}
}

hot void
group_write(Group* self, Stack* stack)
{
	int count = self->keys_count + self->aggr_count;
	Value* data[count];
	for (int i = 0; i < count; i++)
		data[i] = stack_at(stack, count - i);

	Value** target_data = data;
	Value** target_data_aggs = data + self->keys_count;

	// find or create group node, copy group by keys
	auto node = group_find_or_create(self, target_data);

	// process aggrs
	group_write_node(self, node, target_data_aggs);
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
	group_read_aggr(self, node, pos, value);
}

void
group_read_aggr(Group* self, GroupNode* node, int pos, Value* value)
{
	Aggr* aggr = NULL;
	uint8_t* state = (uint8_t*)node + node->aggr_offset;
	int i = 0;
	list_foreach(&self->aggrs)
	{
		aggr = list_at(Aggr, link);
		if (i != pos)
		{
			state += aggr_state_size(aggr);
			i++;
			continue;
		}
		break;
	}
	return aggr_read(aggr, state, value);
}

void
group_read(Group* self, GroupNode* node, Value* result)
{
	if (self->keys_count > 1)
	{
		auto buf = buf_create();
		encode_array(buf);
		for (int i = 0; i < self->keys_count; i++)
			value_write(&node->keys[i], buf);
		encode_array_end(buf);
		value_set_array_buf(result, buf);
	} else {
		value_copy(result, &node->keys[0]);
	}
}
