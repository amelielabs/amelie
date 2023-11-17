
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>

static void
group_free(ValueObj* obj)
{
	auto self = (Group*)obj;
	list_foreach_safe(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_free(aggr);
	}
	list_init(&self->aggrs);

	if (self->ht.count > 0)
	{
		auto index = (GroupNode**)(self->ht.buf.start);
		for (int i = 0; i < self->ht.size; i++)
		{
			if (index[i] == NULL)
				continue;
			for (int j = 0; j < self->keys_count; j++)
				value_free(&index[i]->keys[j]);
			mn_free(index[i]);
		}
	}
	self->aggr_size  = 0;
	self->aggr_count = 0;
	self->keys_count = 0;

	hashtable_free(&self->ht);
	mn_free(self);
}

Group*
group_create(int keys_count)
{
	Group* self = mn_malloc(sizeof(Group));
	self->obj.free    = group_free;
	self->obj.convert = NULL;
	self->aggr_size   = 0;
	self->aggr_count  = 0;
	self->keys_count  = keys_count;
	list_init(&self->aggrs);
	hashtable_init(&self->ht);

	guard(guard, group_free, self);
	hashtable_create(&self->ht, 256);
	return unguard(&guard);
}

void
group_add_aggr(Group* self, AggrIf* iface)
{
	auto aggr = aggr_create(iface);
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
	node = mn_malloc(sizeof(GroupNode) + index_size + key->size + self->aggr_size);
	node->node.hash = key->hash;

	// copy keys
	uint8_t* pos;
	pos = (uint8_t*)node + sizeof(GroupNode) + index_size;
	for (int i = 0; i < self->keys_count; i++)
	{
		auto value = key->target_data[i];
		switch (value->type) {
		case VALUE_INT:
			value_set_int(&node->keys[i], value->integer);
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
		case VALUE_DATA:
			memcpy(pos, value->data, value->data_size);
			value_set_data(&node->keys[i], pos, value->data_size, NULL);
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
			data = &value->integer;
			data_size = sizeof(value->integer);
			break;
		case VALUE_REAL:
			data = &value->real;
			data_size = sizeof(value->real);
			break;
		case VALUE_STRING:
		case VALUE_DATA:
			data = str_of(&value->string);
			data_size = str_size(&value->string);
			key.size += data_size;
			break;
		case VALUE_NULL:
		case VALUE_SET:
		case VALUE_GROUP:
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
group_process(Group* self, GroupNode* node, Value** target_data_aggs)
{
	uint8_t* aggr_state = (uint8_t*)node + node->aggr_offset;
	int i = 0;
	list_foreach(&self->aggrs)
	{
		auto aggr = list_at(Aggr, link);
		aggr_process(aggr, aggr_state, target_data_aggs[i]);
		aggr_state += aggr_state_size(aggr);
		i++;
	}
}

hot void
group_add(Group* self, Stack* stack)
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
	group_process(self, node, target_data_aggs);
}

void
group_get_aggr(Group* self, GroupNode* node, int pos, Value* value)
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
	return aggr_convert(aggr, state, value);
}

Buf*
group_get(Group* self, GroupNode* node)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_array(buf, self->keys_count);
	for (int i = 0; i < self->keys_count; i++)
		value_write(&node->keys[i], buf);
	msg_end(buf);
	return buf;
}
