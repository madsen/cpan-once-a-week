#! /usr/bin/perl
#---------------------------------------------------------------------
# createDB.pl
# Copyright 2012 Christopher J. Madsen
#
# Create the database
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use DBI;
my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 0, PrintError => 0, RaiseError => 1 });

$db->do(<<'');
CREATE TABLE authors (
  author_num     INTEGER PRIMARY KEY,
  author_id      TEXT NOT NULL UNIQUE,
  longest_start  TIMESTAMP,
  longest_length INTEGER NOT NULL DEFAULT 0,
  last_start     TIMESTAMP,
  last_end       TIMESTAMP,
  last_release   TIMESTAMP,
  last_length    INTEGER NOT NULL DEFAULT 0,
  active_weeks   INTEGER NOT NULL DEFAULT 0,
  total_releases INTEGER NOT NULL DEFAULT 0
)

$db->do(<<'');
CREATE TABLE dists (
  dist_id    INTEGER PRIMARY KEY,
  dist_name  TEXT NOT NULL UNIQUE
)

$db->do(<<'');
CREATE TABLE releases (
  release_id INTEGER PRIMARY KEY,
  author_num INTEGER NOT NULL REFERENCES authors,
  dist_id    INTEGER NOT NULL REFERENCES dists,
  version    REAL NOT NULL,
  date       TIMESTAMP NOT NULL,
  UNIQUE(author_num, dist_id, version)
)

$db->do(<<'');
CREATE VIEW author_info AS
SELECT author_num, author_id,
       date(longest_start, 'unixepoch') as longest_start,
       longest_length,
       date(last_start, 'unixepoch') as last_start,
       date(last_end, 'unixepoch') as last_end,
       datetime(last_release, 'unixepoch') AS last_release,
       last_length, active_weeks, total_releases
FROM authors

$db->do(<<'');
CREATE VIEW release_info AS
SELECT release_id, author_id, dist_name, version,
       datetime(date, 'unixepoch') AS date
FROM releases NATURAL JOIN authors NATURAL JOIN dists

$db->commit;
$db->disconnect;
