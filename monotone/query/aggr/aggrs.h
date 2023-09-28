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
	AGGR_AVG
};

extern AggrIf *aggrs[];
