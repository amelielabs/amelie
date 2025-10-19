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

// schema
#include "catalog/schema_config.h"
#include "catalog/schema.h"
#include "catalog/schema_op.h"
#include "catalog/schema_mgr.h"

// table
#include "catalog/table_config.h"
#include "catalog/table.h"
#include "catalog/table_op.h"
#include "catalog/table_mgr.h"
#include "catalog/table_mgr_alter.h"
#include "catalog/table_index.h"

// udf
#include "catalog/udf_config.h"
#include "catalog/udf.h"
#include "catalog/udf_op.h"
#include "catalog/udf_mgr.h"

// catalog
#include "catalog/catalog.h"
#include "catalog/cascade.h"
