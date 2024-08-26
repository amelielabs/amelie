
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

char *linenoise(const char *prompt);
void  linenoiseSetMultiLine(int ml);
int   linenoiseHistorySave(const char *filename);
int   linenoiseHistoryLoad(const char *filename);
int   linenoiseHistoryAdd(const char *line);

void
cli_open(char* path)
{
	linenoiseHistoryLoad(path);
	linenoiseSetMultiLine(true);
}

void
cli_close(char* path)
{
	linenoiseHistorySave(path);
}

bool
cli(char* prompt, Str* input)
{
	auto line = linenoise(prompt);
	if (! line)
		return false;
	linenoiseHistoryAdd(line);
	str_set_allocated(input, line, strlen(line));
	return true;
}
