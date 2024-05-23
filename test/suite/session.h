#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

TestSession*
test_session_new(TestSuite*, TestEnv*, const char*);
void test_session_free(TestSuite*, TestSession*);
void test_session_connect(TestSuite*, TestSession*, const char*);
void test_session_execute(TestSuite*, TestSession*, const char*);
TestSession*
test_session_find(TestSuite*, const char*);
