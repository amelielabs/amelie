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

// db
#include "catalog/db_config.h"
#include "catalog/db.h"
#include "catalog/db_op.h"
#include "catalog/db_mgr.h"

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
