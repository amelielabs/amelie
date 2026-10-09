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

// ddl
#include "catalog/ddl.h"

// user
#include "catalog/api.h"
#include "catalog/apis.h"
#include "catalog/user_config.h"
#include "catalog/user_op.h"
#include "catalog/user.h"
#include "catalog/check.h"

// table
#include "catalog/table_config.h"
#include "catalog/table_op.h"
#include "catalog/table.h"
#include "catalog/table_alter.h"
#include "catalog/table_index.h"

// sidetable
#include "catalog/sidetable_config.h"
#include "catalog/sidetable_op.h"
#include "catalog/sidetable.h"

// udf
#include "catalog/udf_config.h"
#include "catalog/udf_op.h"
#include "catalog/udf.h"

// grant
#include "catalog/rel_op.h"
#include "catalog/rel.h"

// eval
#include "catalog/eval.h"

// catalog
#include "catalog/catalog.h"
#include "catalog/catalog_find.h"
#include "catalog/catalog_check.h"
#include "catalog/catalog_limit.h"

// cascade operations
#include "catalog/cascade.h"

// describe
#include "catalog/describe.h"

// mcp
#include "catalog/catalog_mcp.h"
