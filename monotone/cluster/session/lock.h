#pragma once

//
// monotone
//
// SQL OLTP database
//

void session_lock(Session*, SessionLock);
void session_unlock(Session*);
