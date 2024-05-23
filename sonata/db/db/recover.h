#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void recover(Db*, Uuid*);
void recover_wal(Db*);
