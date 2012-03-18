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
SELECT date FROM releases WHERE author_num = ? ORDER BY date

my $update = $db->prepare(<<'');
UPDATE authors SET
  con_longest_start = ?, con_longest_length = ?,
  con_last_start = ?, con_last_end = ?, con_last_length = ?,
  con_active_weeks = ?, con_total_releases = ?,
  hist_longest_start = ?, hist_longest_length = ?,
  hist_last_start = ?, hist_last_end = ?, hist_last_length = ?,
  hist_active_weeks = ?, hist_total_releases = ?,
  last_release = ?
  WHERE author_num = ?

my $con_seinfeld = DateTimeX::Seinfeld->new(
  start_date => {qw(year 2012 month 1 day 1 time_zone UTC)},
  increment  => { weeks => 1 },
);

my $hist_seinfeld = DateTimeX::Seinfeld->new(
  start_date => {qw(year 1995 month 8 day 13 time_zone UTC)},
  increment  => { weeks => 1 },
);

my $contest_start = $con_seinfeld->start_date;

my $count;
foreach my $aNum (@$author_nums) {
  my $dates = $db->selectcol_arrayref($getReleases, undef, $aNum);

  $_ = DateTime->from_epoch(epoch => $_) for @$dates;

  my @hist = find_chains($hist_seinfeld, $dates, 1);

  shift @$dates while @$dates and $dates->[0] < $contest_start;

  $update->execute(find_chains($con_seinfeld, $dates), @hist, $aNum);

  if (++$count > 99) {
    say $aNum;
    $db->commit;
    $count = 0;
  }
} # end foreach $aNum

#---------------------------------------------------------------------
sub find_chains
{
  my ($seinfeld, $dates, $want_last_release) = @_;

  if (@$dates) {

    my $info = $seinfeld->find_chains( $dates );

    return (
      $info->{longest}{start_period}->epoch, $info->{longest}{length},
      $info->{last}{start_period}->epoch, $info->{last}{end_period}->epoch,
      $info->{last}{length},
      $info->{marked_periods}, scalar @$dates,
      $want_last_release ? $info->{last}{end_event}->epoch : ()
    );
  } else {
    # No releases!  Clear the fields:
    return (undef, 0, undef, undef, 0, 0, 0, $want_last_release ? undef : () );
  }
} # end find_chains

$db->commit;
