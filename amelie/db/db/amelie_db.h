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

// schema
#include "db/schema_config.h"
#include "db/schema.h"
#include "db/schema_op.h"
#include "db/schema_mgr.h"

// table
#include "db/table_config.h"
#include "db/table.h"
#include "db/table_op.h"
#include "db/table_mgr.h"
#include "db/table_mgr_alter.h"

// index
#include "db/table_index.h"

// view
#include "db/view_config.h"
#include "db/view.h"
#include "db/view_op.h"
#include "db/view_mgr.h"

// node
#include "db/node_config.h"
#include "db/node.h"
#include "db/node_op.h"
#include "db/node_mgr.h"

// db
#include "db/db.h"
#include "db/db_checkpoint.h"
#include "db/cascade.h"

// recover
#include "db/recover_checkpoint.h"
#include "db/recover.h"
