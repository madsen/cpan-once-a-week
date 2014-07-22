#! /usr/local/bin/perl
#---------------------------------------------------------------------
# backup.pl
# Copyright 2012 Christopher J. Madsen
#
# Back up the database to CSV files so it can be stored in Git
#
# Only the release data is stored; you have to run recalc.pl to
# recalculate the Seinfeld chain data.
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use autodie ':io';

use DateTime ();
use DBI ();
use File::Path 'make_path';
use Text::CSV ();

#---------------------------------------------------------------------
mkdir 'data' unless -d 'data';

my $csv = Text::CSV->new({ binary => 1, eol => "\x0A" })
    or die "Cannot use CSV: " . Text::CSV->error_diag;

my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 1, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

backup(qw(authors  author_num  author_num author_id));

my ($minDate, $maxDate) = $db->selectrow_array(
  'SELECT MIN(date), MAX(date) FROM releases'
);

for ($minDate, $maxDate) { $_ = DateTime->from_epoch( epoch => $_ ) }

$minDate->truncate( to => 'month');
my @releaseArgs = (qw(releases date), [qw(author_num filename date)]);

while ($minDate <= $maxDate) {
  my $endDate = $minDate->clone->add(months => 1);

  my $year = $minDate->year;
  make_path("data/releases/$year");

  backup_where(sprintf("date >= %d AND date < %d",
                       $minDate->epoch, $endDate->epoch),
               "releases/$year/releases-" . $minDate->format_cldr('yyyy-MM'),
               @releaseArgs);

  $minDate = $endDate;
}

$db->disconnect;

#---------------------------------------------------------------------
sub backup_where
{
  my ($where, $basename, $table, $order_by, $fields) = @_;

  open my $fh, ">:utf8", "data/$basename.csv";

  $csv->print($fh, $fields);   # print header row

  $fields = join(',', @$fields);

  my $s = $db->prepare("SELECT $fields FROM $table WHERE $where ORDER BY $order_by");
  $s->execute;

  while (my $row = $s->fetchrow_arrayref) {
    $csv->print($fh, $row);
  }

  close $fh;
} # end backup_where

#---------------------------------------------------------------------
sub backup
{
  my ($table, $order_by, @fields) = @_;

  backup_where(1, $table, $table, $order_by, \@fields);
} # end backup
