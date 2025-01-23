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

// function manager
#include "func/function.h"
#include "func/function_mgr.h"
#include "func/call.h"
#include "func/call_mgr.h"

// functions
#include "func/fn_system.h"
#include "func/fn_cast.h"
#include "func/fn_null.h"
#include "func/fn_json.h"
#include "func/fn_string.h"
#include "func/fn_regexp.h"
#include "func/fn_math.h"
#include "func/fn_misc.h"
#include "func/fn_time.h"
#include "func/fn_vector.h"
#include "func/fn.h"
