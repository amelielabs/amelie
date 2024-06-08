#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void recover_cmd(Db*, Transaction*, uint8_t**, uint8_t**);
void recover(Db*, Uuid*);
void recover_wal(Db*);
