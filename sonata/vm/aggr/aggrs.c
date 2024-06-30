
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

AggrIf* aggrs[] =
{
	[AGGR_COUNT]  = &aggr_count,
	[AGGR_SUM]    = &aggr_sum,
	[AGGR_AVG]    = &aggr_avg,
	[AGGR_MIN]    = &aggr_min,
	[AGGR_MAX]    = &aggr_max,
	[AGGR_LAMBDA] = &aggr_lambda
};
