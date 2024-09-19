#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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

// index
#include "db/table_index.h"

// view
#include "db/view_config.h"
#include "db/view.h"
#include "db/view_op.h"
#include "db/view_mgr.h"

// udf
#include "db/udf_config.h"
#include "db/udf.h"
#include "db/udf_op.h"
#include "db/udf_mgr.h"

// db
#include "db/db.h"
#include "db/db_checkpoint.h"
#include "db/cascade.h"

// recover
#include "db/recover_checkpoint.h"
#include "db/recover.h"
