#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline Stmt*
analyze_stmts(Parser* parser, int* count)
{
	*count = 0;
	Stmt* last = NULL;
	list_foreach(&parser->stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		if (target_list_has(&stmt->target_list, TARGET_TABLE) ||
		    target_list_has(&stmt->target_list, TARGET_TABLE_SHARED))
		{
			last = stmt;
			(*count)++;
		}
	}
	return last;
}

static inline uint32_t
target_lookup_hash(Target* target)
{
	uint32_t hash = 0;
	list_foreach(&target->index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &ast_path_of(target->path)->keys[key->order];
		assert(ref->start);
		if (key->type == TYPE_STRING)
			hash = key_hash_string(hash, &ref->start->string);
		else
		if (key->type == TYPE_TIMESTAMP ||
		    key->type == TYPE_TIMESTAMPTZ)
			hash = key_hash_timestamp(hash, ref->start->integer);
		else
			hash = key_hash_integer(hash, ref->start->integer);
	}
	return hash;
}
