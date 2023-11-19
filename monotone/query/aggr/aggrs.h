#pragma once

//
// monotone
//
// SQL OLTP database
//

enum
{
	AGGR_COUNT,
	AGGR_SUM,
	AGGR_AVG,
	AGGR_MIN,
	AGGR_MAX
};

extern AggrIf *aggrs[];
