
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_append(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		error("append(): expected two or more arguments");
	value_append(self->result, argv[0], self->argc - 1, argv + 1);
}

hot static void
fn_push(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2)
		error("push(): expected two or more arguments");
	call_validate_arg(self, 0, VALUE_ARRAY);
	auto data = argv[0]->data;
	auto data_size = argv[0]->data_size;
	value_array_push(self->result, data, data_size, self->argc - 1, argv + 1);
}

hot static void
fn_pop(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	call_validate_arg(self, 0, VALUE_ARRAY);
	auto data = argv[0]->data;
	auto data_size = argv[0]->data_size;
	value_array_pop(self->result, data, data_size);
}

hot static void
fn_pop_back(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 1);
	call_validate_arg(self, 0, VALUE_ARRAY);
	auto data = argv[0]->data;
	value_array_pop_back(self->result, data);
}

hot static void
fn_put(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	call_validate_arg(self, 0, VALUE_ARRAY);
	call_validate_arg(self, 1, VALUE_INT);
	auto data = argv[0]->data;
	value_array_put(self->result, data, argv[1]->integer, argv[2]);
}

hot static void
fn_remove(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_ARRAY);
	call_validate_arg(self, 1, VALUE_INT);
	auto data = argv[0]->data;
	value_array_remove(self->result, data, argv[1]->integer);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 3);
	value_set(self->result, argv[0], argv[1], argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	value_unset(self->result, argv[0], argv[1]);
}

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	value_has(self->result, argv[0], argv[1]);
}

FunctionDef fn_object_def[] =
{
	// array
	{ "public", "append",    fn_append,   false },
	{ "public", "push_back", fn_append,   false },
	{ "public", "push",      fn_push,     false },
	{ "public", "pop",       fn_pop,      false },
	{ "public", "pop_back",  fn_pop_back, false },
	{ "public", "put",       fn_put,      false },
	{ "public", "remove",    fn_remove,   false },
	// object
	{ "public", "set",       fn_set,      false },
	{ "public", "unset",     fn_unset,    false },
	{ "public", "has",       fn_has,      false },
	{  NULL,     NULL,       NULL,        false }
};
