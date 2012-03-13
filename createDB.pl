#! /usr/bin/perl
#---------------------------------------------------------------------
# createDB.pl
# Copyright 2012 Christopher J. Madsen
#
# Create the database and reload release data
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use autodie ':io';

use DBI;
use Text::CSV ();

my $dbname = 'seinfeld.db';

die "$dbname already exists!\n" if -e $dbname;

my $db = DBI->connect("dbi:SQLite:dbname=$dbname","","",
                      { AutoCommit => 0, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

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

#---------------------------------------------------------------------
my $csv = Text::CSV->new({ binary => 1, eol => "\x0A" })
    or die "Cannot use CSV: " . Text::CSV->error_diag;

for my $table (qw(authors dists releases)) {
  my $fn = "data/$table.csv";

  die "Unable to restore $table: $fn not found\n" unless -e $fn;
  open my $fh, "<:utf8", $fn;

  say "Loading data from $fn...";

  my $fields = $csv->getline($fh) or die "Can't read header";

  my $field_list   = join(',', @$fields);
  my $placeholders = join(',', ('?') x @$fields);

  my $insert = $db->prepare(
    "INSERT INTO $table ($field_list) VALUES ($placeholders)"
  );

  while (my $row = $csv->getline($fh)) {
    $insert->execute(@$row);
  }

  $db->commit;

  close $fh;
} # end backup

#---------------------------------------------------------------------
say 'Release data restored.  Now run recalc.pl to find Seinfeld chains.';

$db->disconnect;
