#pragma once

//
// monotone
//
// SQL OLTP database
//

void recover_write(Db*, uint64_t, uint8_t*, bool);
void recover(Db*);
