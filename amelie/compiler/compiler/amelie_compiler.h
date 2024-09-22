#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// register mapping
#include "compiler/rmap.h"

// compiler
#include "compiler/compiler.h"
#include "compiler/compiler_op.h"

// scan
#include "compiler/scan.h"

// emit
#include "compiler/emit_expr.h"
#include "compiler/emit_insert.h"
#include "compiler/emit_update.h"
#include "compiler/emit_delete.h"
#include "compiler/emit_upsert.h"
#include "compiler/emit_select.h"
#include "compiler/emit_watch.h"
#include "compiler/pushdown.h"
