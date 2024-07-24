
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
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

AggrIf* aggrs[] =
{
	[AGGR_COUNT]  = &aggr_count,
	[AGGR_SUM]    = &aggr_sum,
	[AGGR_AVG]    = &aggr_avg,
	[AGGR_MIN]    = &aggr_min,
	[AGGR_MAX]    = &aggr_max,
	[AGGR_LAMBDA] = &aggr_lambda
};
