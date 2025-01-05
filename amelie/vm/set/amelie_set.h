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
#include "set/set_hash.h"
#include "set/set.h"
#include "set/set_cache.h"
#include "set/set_compare.h"
#include "set/set_iterator.h"

// aggregate
#include "set/agg.h"

// union
#include "set/union.h"
#include "set/union_iterator.h"

// any/all/in
#include "set/any.h"
#include "set/all.h"
#include "set/in.h"
#include "set/exists.h"
