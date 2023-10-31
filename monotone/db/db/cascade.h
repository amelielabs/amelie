#pragma once

//
// monotone
//
// SQL OLTP database
//

void cascade_schema_drop(Db*, Transaction*, Str*, bool, bool);
void cascade_schema_rename(Db*, Transaction*, Str*, Str*, bool);
