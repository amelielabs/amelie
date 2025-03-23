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

// register mapping
#include "compiler/rmap.h"

// compiler
#include "compiler/compiler.h"
#include "compiler/compiler_op.h"

// scan
#include "compiler/path.h"
#include "compiler/path_prepare.h"
#include "compiler/scan.h"

// emit
#include "compiler/cast.h"
#include "compiler/emit_expr.h"
#include "compiler/emit_insert.h"
#include "compiler/emit_upsert.h"
#include "compiler/emit_update.h"
#include "compiler/emit_delete.h"
#include "compiler/emit_select.h"
#include "compiler/emit_watch.h"
#include "compiler/pushdown.h"
