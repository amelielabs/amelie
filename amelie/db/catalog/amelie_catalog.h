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

// catalog
#include "catalog/ddl.h"
#include "catalog/catalog.h"

// user
#include "catalog/api.h"
#include "catalog/apis.h"
#include "catalog/user_config.h"
#include "catalog/user_op.h"
#include "catalog/user.h"

// table
#include "catalog/table_config.h"
#include "catalog/table_op.h"
#include "catalog/table.h"
#include "catalog/table_alter.h"
#include "catalog/table_index_create.h"
#include "catalog/table_index.h"

// udf
#include "catalog/udf_config.h"
#include "catalog/udf_op.h"
#include "catalog/udf.h"

// rel
#include "catalog/rel_op.h"
#include "catalog/rel.h"

// catalog
#include "catalog/cascade.h"
#include "catalog/describe.h"

// mcp
#include "catalog/mcp.h"

// execute
#include "catalog/execute.h"
