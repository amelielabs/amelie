
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
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
	call_validate_arg(self, 0, VALUE_DATA);
	auto data = argv[0]->data;
	auto data_size = argv[0]->data_size;
	if (! data_is_array(data))
		error("push(): array expected");
	value_array_push(self->result, data, data_size, self->argc - 1, argv + 1);
}

hot static void
fn_pop(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_DATA);
	auto data = argv[0]->data;
	auto data_size = argv[0]->data_size;
	if (! data_is_array(data))
		error("pop(): array expected");
	value_array_pop(self->result, data, data_size);
}

hot static void
fn_pop_back(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_DATA);
	auto data = argv[0]->data;
	if (! data_is_array(data))
		error("pop_back(): array expected");
	value_array_pop_back(self->result, data);
}

hot static void
fn_put(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_DATA);
	call_validate_arg(self, 1, VALUE_INT);
	auto data = argv[0]->data;
	if (! data_is_array(data))
		error("remove(): array expected");
	value_array_put(self->result, data, argv[1]->integer, argv[2]);
}

hot static void
fn_remove(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	call_validate_arg(self, 0, VALUE_DATA);
	call_validate_arg(self, 1, VALUE_INT);
	auto data = argv[0]->data;
	if (! data_is_array(data))
		error("remove(): array expected");
	value_array_remove(self->result, data, argv[1]->integer);
}

hot static void
fn_set(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_set(self->result, argv[0], argv[1], argv[2]);
}

hot static void
fn_unset(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_unset(self->result, argv[0], argv[1]);
}

hot static void
fn_has(Call* self)
{
	auto argv = self->argv;
	call_validate(self);
	value_has(self->result, argv[0], argv[1]);
}

FunctionDef fn_object_def[] =
{
	// array
	{ "public", "append",    fn_append,   2 },
	{ "public", "push_back", fn_append,   2 },
	{ "public", "push",      fn_push,     2 },
	{ "public", "pop",       fn_pop,      1 },
	{ "public", "pop_back",  fn_pop_back, 1 },
	{ "public", "put",       fn_put,      3 },
	{ "public", "remove",    fn_remove,   2 },
	/// map
	{ "public", "set",       fn_set,      3 },
	{ "public", "unset",     fn_unset,    2 },
	{ "public", "has",       fn_has,      2 },
	{  NULL,     NULL,       NULL,        0 }
};
