#! /bin/sh

exec rsync "$@" -rlptgDv --delete '--filter=. filter.txt' web/ onceaweek:.
