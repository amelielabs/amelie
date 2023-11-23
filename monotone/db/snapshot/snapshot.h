#pragma once

//
// monotone
//
// SQL OLTP database
//

pid_t snapshot(StorageMgr*, Condition*, Uuid*);
