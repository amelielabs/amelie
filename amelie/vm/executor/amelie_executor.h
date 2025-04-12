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

// bytecode
#include "executor/code_data.h"
#include "executor/code.h"
#include "executor/program.h"

// request
#include "executor/error.h"
#include "executor/req.h"
#include "executor/req_list.h"
#include "executor/req_cache.h"
#include "executor/req_queue.h"

// pipe
#include "executor/pipe.h"
#include "executor/pipe_cache.h"
#include "executor/pipe_set.h"

// executor
#include "executor/dispatch.h"
#include "executor/dtr.h"
#include "executor/prepare.h"
#include "executor/executor.h"
