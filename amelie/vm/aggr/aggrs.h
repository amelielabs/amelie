#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

enum
{
	AGGR_COUNT,
	AGGR_SUM,
	AGGR_AVG,
	AGGR_MIN,
	AGGR_MAX,
	AGGR_LAMBDA
};

extern AggrIf* aggrs[];
