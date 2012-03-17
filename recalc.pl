#! /usr/bin/perl
#---------------------------------------------------------------------
# recalc.pl
# Copyright 2012 Christopher J. Madsen
#
# Regenerate the Seinfeld chains from scratch
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use DateTimeX::Seinfeld 0.02 ();
use DBI ();

my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 0, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

my $author_nums = $db->selectcol_arrayref('SELECT author_num FROM authors');

my $getReleases = $db->prepare(<<'');
SELECT date FROM releases WHERE author_num = ? AND date >= 1325376000
  ORDER BY date

my $update = $db->prepare(<<'');
UPDATE authors SET longest_start = ?, longest_length = ?,
  last_start = ?, last_end = ?,
  last_release = ?, last_length = ?,
  active_weeks = ?, total_releases = ?
  WHERE author_num = ?

my $seinfeld = DateTimeX::Seinfeld->new(
  start_date => {qw(year 2012 month 1 day 1 time_zone UTC)},
  increment  => { weeks => 1 },
);

foreach my $aNum (@$author_nums) {
  my $dates = $db->selectcol_arrayref($getReleases, undef, $aNum);

  if (@$dates) {
    $_ = DateTime->from_epoch(epoch => $_) for @$dates;

    my $info = $seinfeld->find_chains( $dates );

    $update->execute(
      $info->{longest}{start_period}->epoch, $info->{longest}{length},
      $info->{last}{start_period}->epoch, $info->{last}{end_period}->epoch,
      $info->{last}{end_event}->epoch, $info->{last}{length},
      $info->{marked_periods}, scalar @$dates, $aNum
    );
  } else {
    # No releases!  Clear the fields:
    $update->execute(undef, 0, undef, undef, undef, 0, 0, 0, $aNum);
  }
} # end foreach $aNum

$db->commit;
