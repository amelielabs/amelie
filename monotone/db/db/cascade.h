#pragma once

//
// monotone
//
// SQL OLTP database
//

void cascade_schema_drop(Db*, Transaction*, Str*, bool, bool);
void cascade_schema_alter(Db*, Transaction*, Str*, SchemaConfig*, bool);
