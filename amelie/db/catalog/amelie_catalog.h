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

// storage
#include "catalog/storage_op.h"

// database
#include "catalog/database_config.h"
#include "catalog/database.h"
#include "catalog/database_op.h"
#include "catalog/database_mgr.h"

// table
#include "catalog/table_config.h"
#include "catalog/table.h"
#include "catalog/table_op.h"
#include "catalog/table_mgr.h"
#include "catalog/table_mgr_alter.h"
#include "catalog/table_index.h"
#include "catalog/table_tier.h"

// udf
#include "catalog/udf_config.h"
#include "catalog/udf.h"
#include "catalog/udf_op.h"
#include "catalog/udf_mgr.h"

// catalog
#include "catalog/catalog.h"
#include "catalog/catalog_file.h"

// catalog snapshot
#include "catalog/catalog_snapshot.h"

// cascade operations
#include "catalog/cascade.h"
