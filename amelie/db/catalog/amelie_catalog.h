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

// grant
#include "catalog/grant_op.h"

// storage
#include "catalog/storage_op.h"

// user
#include "catalog/user_config.h"
#include "catalog/user_op.h"
#include "catalog/user.h"
#include "catalog/check.h"

// table
#include "catalog/table_config.h"
#include "catalog/table_op.h"
#include "catalog/table.h"
#include "catalog/table_alter.h"
#include "catalog/table_storage.h"
#include "catalog/table_index.h"

// branch
#include "catalog/branch_config.h"
#include "catalog/branch_op.h"
#include "catalog/branch.h"

// udf
#include "catalog/udf_config.h"
#include "catalog/udf_op.h"
#include "catalog/udf.h"

// topic
#include "catalog/topic_config.h"
#include "catalog/topic_op.h"
#include "catalog/topic.h"
#include "catalog/publish.h"

// subscription
#include "catalog/sub_config.h"
#include "catalog/sub_op.h"
#include "catalog/sub.h"
#include "catalog/acknowledge.h"

// catalog
#include "catalog/catalog.h"
#include "catalog/catalog_find.h"
#include "catalog/catalog_file.h"
#include "catalog/catalog_snapshot.h"
#include "catalog/catalog_ref.h"

// cascade operations
#include "catalog/cascade.h"
