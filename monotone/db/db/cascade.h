#pragma once

//
// indigo
//
// SQL OLTP database
//

void cascade_table_drop(Db*, Transaction*, Str*, Str*, bool);
void cascade_schema_drop(Db*, Transaction*, Str*, bool, bool);
void cascade_schema_rename(Db*, Transaction*, Str*, Str*, bool);
