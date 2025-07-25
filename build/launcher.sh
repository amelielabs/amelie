#!/bin/bash

# compiler call wrapper
#
# extract CC -c <path_compile> and -o <path_output>
#
path_compile=
path_output=
for ((arg=1; arg<=$#; arg++)); do
	next=$((arg+1))
	if [ "${!arg}" == "-c" ]; then
		path_compile="${!next}"
	elif [ "${!arg}" == "-o" ]; then
		path_output="${!next}"
	fi
done

tool=`basename $1`

# linker or compiler call
if [ -z "$path_compile" ]; then
	echo "$tool -o $path_output"
else
	# shorten path to <parent_dir>/<file>
	path_rel=`echo ${path_compile} | awk -F "/" '{ print $(NF-2) "/" $(NF-1) "/" $NF }'`
	echo "$tool -c ${path_rel}"
fi

exec "$@"
