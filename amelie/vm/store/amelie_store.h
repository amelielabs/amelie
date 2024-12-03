#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

// set
#include "store/set_hash.h"
#include "store/set.h"
#include "store/set_cache.h"
#include "store/set_compare.h"
#include "store/set_iterator.h"

// merge
#include "store/merge.h"
#include "store/merge_iterator.h"

// aggregate
#include "store/agg.h"

// any/all/in
#include "store/any.h"
#include "store/all.h"
#include "store/in.h"

// body
#include "store/body.h"
