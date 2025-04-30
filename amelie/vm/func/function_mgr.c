
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

void
function_mgr_init(FunctionMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
	hashtable_init(&self->ht);
}

void
function_mgr_free(FunctionMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto func = list_at(Function, link);
		function_free(func);
	}
	hashtable_free(&self->ht);
}

void
function_mgr_add(FunctionMgr* self, Function* func)
{
	// add function to the list
	list_append(&self->list, &func->link);
	self->list_count++;

	// add function to the hash table
	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 256);
	hashtable_reserve(&self->ht);

	// hash by schema/name
	uint32_t hash;
	hash = hash_murmur3_32(str_u8(&func->schema), str_size(&func->schema), 0);
	hash = hash_murmur3_32(str_u8(&func->name), str_size(&func->name), hash);
	func->link_ht.hash = hash;
	hashtable_set(&self->ht, &func->link_ht);
}

void
function_mgr_del(FunctionMgr* self, Function* func)
{
	hashtable_delete(&self->ht, &func->link_ht);

	list_unlink(&func->link);
	self->list_count--;
}

hot static inline bool
function_mgr_cmp(HashtableNode* node, void* ptr)
{
	Str** target = ptr;
	auto func = container_of(node, Function, link_ht);
	return (str_compare(&func->schema, target[0]) &&
	        str_compare(&func->name,   target[1]));
}

hot Function*
function_mgr_find(FunctionMgr* self, Str* schema, Str* name)
{
	uint32_t hash;
	hash = hash_murmur3_32(str_u8(schema), str_size(schema), 0);
	hash = hash_murmur3_32(str_u8(name), str_size(name), hash);
	Str* target[] = { schema, name };
	auto node = hashtable_get(&self->ht, hash, function_mgr_cmp, target);
	if (likely(node))
		return container_of(node, Function, link_ht);
	return NULL;
}
