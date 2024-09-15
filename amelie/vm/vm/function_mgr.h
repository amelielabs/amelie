#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct FunctionMgr FunctionMgr;

struct FunctionMgr
{
	List list;
	int  list_count;
};

static inline void
function_mgr_init(FunctionMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
function_mgr_free(FunctionMgr* self)
{
	list_foreach_safe(&self->list) {
		auto func = list_at(Function, link);
		function_free(func);
	}
}

static inline void
function_mgr_add(FunctionMgr* self, Function* func)
{
	list_append(&self->list, &func->link);
	self->list_count++;
}

static inline void
function_mgr_register(FunctionMgr* self, FunctionDef* list)
{
	for (int i = 0; list[i].name; i++)
	{
		auto func = function_allocate(&list[i]);
		function_mgr_add(self, func);
	}
}

static inline Function*
function_mgr_find(FunctionMgr* self, Str* schema, Str* name)
{
	list_foreach(&self->list)
	{
		auto func = list_at(Function, link);
		if (! str_compare(&func->schema, schema))
			continue;
		if (str_compare(&func->name, name))
			return func;
	}
	return NULL;
}

static inline Buf*
function_mgr_list(FunctionMgr* self)
{
	auto buf = buf_create();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto func = list_at(Function, link);
		function_write(func, buf);
	}
	encode_array_end(buf);
	return buf;
}
