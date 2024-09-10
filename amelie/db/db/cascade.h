#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

void cascade_table_drop(Db*, Tr*, Str*, Str*, bool);
void cascade_schema_drop(Db*, Tr*, Str*, bool, bool);
void cascade_schema_rename(Db*, Tr*, Str*, Str*, bool);
