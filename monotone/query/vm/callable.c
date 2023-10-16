
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_request.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>

static void
callable_init(Meta* self, void* arg)
{
	unused(arg);
	auto callable = (Callable*)mn_malloc(sizeof(Callable));
	callable->meta = self;
	code_init(&callable->code);
	code_data_init(&callable->code_data);
	
	Exception e;
	if (try(&e))
	{
		uint8_t* pos = str_u8(&self->config->data);
		code_load(&callable->code, &callable->code_data, &pos);
	}

	if (catch(&e))
	{
		code_free(&callable->code);
		code_data_free(&callable->code_data);
		mn_free(callable);
		rethrow();
	}
	self->iface_data = callable;
}

static void
callable_free(Meta* self)
{
	Callable* callable = self->iface_data;
	if (callable == NULL)
		return;
	code_free(&callable->code);
	code_data_free(&callable->code_data);
	mn_free(callable);
	self->iface_data = NULL;
}

MetaIf callable_if =
{
	.init = callable_init,
	.free = callable_free
};

static inline void
callable_portal_to_set(Portal* portal, Buf* buf)
{
	Set* set = portal->arg;
	Value value;
	value_set_data_from(&value, buf);
	set_add(set, &value, NULL);
}

void
call(Vm*       vm,
     Value*    result,
     Callable* callable,
     int       argc,
     Value**   argv)
{
	auto meta = callable->meta;
	auto schema = &meta->config->schema;
	if (argc != schema->column_count)
		error("%.*s: expects %d arguments", str_size(&meta->config->name),
		      str_of(&meta->config->name),
		      schema->column_count);

	// prepare result set and portal
	auto set = set_create(NULL);

	Portal call_portal;
	portal_init(&call_portal, callable_portal_to_set, set);

	// prepare vm and execute callable
	Vm call_vm;
	vm_init(&call_vm, vm->db, vm->function_mgr, vm->shard);
	guard(vm_guard, vm_free, &call_vm);

	Exception e;
	if (try(&e))
	{
		vm_run(&call_vm, vm->trx, vm->command,
		       &callable->code,
		       &callable->code_data, argc, argv, &call_portal);
	}

	if (catch(&e))
	{
		set->obj.free(&set->obj);
		rethrow();
	}

	// set result
	value_set_set(result, &set->obj);
}
