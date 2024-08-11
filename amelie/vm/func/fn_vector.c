
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
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_cos_distance(Call* self)
{
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_VECTOR);
	call_validate_arg(self, 1, VALUE_VECTOR);
	if (argv[0]->vector.size != argv[1]->vector.size)
		error("cos_distance(): vector sizes does not match");
	auto distance = vector_distance(&argv[0]->vector, &argv[1]->vector);
	value_set_real(self->result, distance);
}

FunctionDef fn_vector_def[] =
{
	{ "public", "cos_distance", fn_cos_distance, false },
	{  NULL,     NULL,          NULL,            false }
};
