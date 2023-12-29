#pragma once

//
// indigo
//
// SQL OLTP database
//

Buf* wal_debug_list_files(Wal*);
Buf* wal_debug_flush(Wal*);
Buf* wal_debug_gc(Wal*, uint64_t);
Buf* wal_debug_read(Wal*, uint64_t);
Buf* wal_debug_snapshot(Wal*, uint64_t);
Buf* wal_debug_snapshot_del(Wal*, uint64_t);
