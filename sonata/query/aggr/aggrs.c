
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>

AggrIf* aggrs[] =
{
	[AGGR_COUNT] = &aggr_count,
	[AGGR_SUM]   = &aggr_sum,
	[AGGR_AVG]   = &aggr_avg,
	[AGGR_MIN]   = &aggr_min,
	[AGGR_MAX]   = &aggr_max
};
