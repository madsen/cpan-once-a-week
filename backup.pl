#! /usr/bin/perl
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

use DBI ();
use Text::CSV ();

#---------------------------------------------------------------------
mkdir 'data' unless -d 'data';

my $csv = Text::CSV->new({ binary => 1, eol => "\x0A" })
    or die "Cannot use CSV: " . Text::CSV->error_diag;

my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 1, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

backup(qw(authors  author_num  author_num author_id));
backup(qw(contests contest_id  contest_id contest_name start_date end_date));
backup(qw(releases release_id  author_num filename date));

$db->disconnect;

#---------------------------------------------------------------------
sub backup
{
  my ($table, $order_by, @fields) = @_;

  open my $fh, ">:utf8", "data/$table.csv";

  $csv->print($fh, \@fields);   # print header row

  my $fields = join(',', @fields);

  my $s = $db->prepare("SELECT $fields FROM $table ORDER BY $order_by");
  $s->execute;

  while (my $row = $s->fetchrow_arrayref) {
    $csv->print($fh, $row);
  }

  close $fh;
} # end backup
