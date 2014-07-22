#! /usr/local/bin/perl
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
use Path::Tiny 'path';
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
  con_longest_start   TIMESTAMP,
  con_longest_length  INTEGER NOT NULL DEFAULT 0,
  con_last_start      TIMESTAMP,
  con_last_end        TIMESTAMP,
  con_last_length     INTEGER NOT NULL DEFAULT 0,
  con_active_weeks    INTEGER NOT NULL DEFAULT 0,
  con_total_releases  INTEGER NOT NULL DEFAULT 0,
  hist_longest_start  TIMESTAMP,
  hist_longest_length INTEGER NOT NULL DEFAULT 0,
  hist_last_start     TIMESTAMP,
  hist_last_end       TIMESTAMP,
  hist_last_length    INTEGER NOT NULL DEFAULT 0,
  hist_active_weeks   INTEGER NOT NULL DEFAULT 0,
  hist_total_releases INTEGER NOT NULL DEFAULT 0,
  last_release        TIMESTAMP
)

$db->do(<<'');
CREATE TABLE releases (
  release_id INTEGER PRIMARY KEY,
  author_num INTEGER NOT NULL REFERENCES authors,
  filename   TEXT NOT NULL,
  date       TIMESTAMP NOT NULL,
  UNIQUE(author_num, filename)
)

$db->do(<<'');
CREATE VIEW author_contest AS
SELECT author_num, author_id,
       date(con_longest_start, 'unixepoch') AS longest_start,
       con_longest_length AS longest_length,
       date(con_last_start, 'unixepoch') AS last_start,
       date(con_last_end, 'unixepoch') AS last_end,
       datetime(last_release, 'unixepoch') AS last_release,
       con_last_length AS last_length,
       con_active_weeks AS active_weeks,
       con_total_releases AS total_releases
FROM authors

$db->do(<<'');
CREATE VIEW author_hist AS
SELECT author_num, author_id,
       date(hist_longest_start, 'unixepoch') AS longest_start,
       hist_longest_length AS longest_length,
       date(hist_last_start, 'unixepoch') AS last_start,
       date(hist_last_end, 'unixepoch') AS last_end,
       datetime(last_release, 'unixepoch') AS last_release,
       hist_last_length AS last_length,
       hist_active_weeks AS active_weeks,
       hist_total_releases AS total_releases
FROM authors

$db->do(<<'');
CREATE VIEW author_info AS
SELECT author_num, author_id,
       date(con_longest_start, 'unixepoch') as con_longest_start,
       con_longest_length,
       date(con_last_start, 'unixepoch') as con_last_start,
       date(con_last_end, 'unixepoch') as con_last_end,
       con_last_length, con_active_weeks, con_total_releases,
       date(hist_longest_start, 'unixepoch') as hist_longest_start,
       hist_longest_length,
       date(hist_last_start, 'unixepoch') as hist_last_start,
       date(hist_last_end, 'unixepoch') as hist_last_end,
       hist_last_length, hist_active_weeks, hist_total_releases,
       datetime(last_release, 'unixepoch') AS last_release
FROM authors

$db->do(<<'');
CREATE VIEW release_info AS
SELECT release_id, author_id, filename,
       datetime(date, 'unixepoch') AS date
FROM releases NATURAL JOIN authors

$db->commit;

#---------------------------------------------------------------------
my $csv = Text::CSV->new({ binary => 1, eol => "\x0A" })
    or die "Cannot use CSV: " . Text::CSV->error_diag;

restore(authors => "data/authors.csv");

for my $dir (sort (path(qw(data releases))->children(qr/^\d{4}$/))) {
  for my $fn (sort $dir->children(qr/^releases-\d{4}-\d\d\.csv$/)) {
    restore(releases => $fn->stringify);
  }
}

sub restore {
  my ($table, $fn) = @_;

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
} # end restore

#---------------------------------------------------------------------
say 'Release data restored.  Now run recalc.pl to find Seinfeld chains.';

$db->disconnect;
