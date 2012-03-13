#! /bin/sh

rsync "$@" -rlptgDv --delete '--filter=. filter.txt' web/ onceaweek:.
