#---------------------------------------------------------------------
package Contest_Util;
#
# Copyright 2012 Christopher J. Madsen
#
# This program is free software; you can redistribute it and/or modify
# it under the same terms as Perl itself.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See either the
# GNU General Public License or the Artistic License for more details.
#
# ABSTRACT: Utility functions for the CPAN Once-a-Week contest
#---------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

use DateTimeX::Seinfeld 0.02 ();
use DBI ();

use Exporter 5.57 'import';     # exported import method
our @EXPORT = qw(@contests $db db_init load_contests process_authors);
our @EXPORT_OK = qw();

our (
  @contests, $db,
  $get_last_release, $get_previous_chains, $get_releases,
  $update_last_release,
  $update_standings, $delete_standings,
);

#=====================================================================
sub db_init
{
  $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                     { AutoCommit => 0, PrintError => 0, RaiseError => 1 });
  $db->do("PRAGMA foreign_keys = ON");
  $db->{FetchHashKeyName} = 'NAME_lc';
} # end db_init

#---------------------------------------------------------------------
# Read information about each contest:

sub load_contests
{
  my $get_contests = $db->prepare(<<'');
  SELECT contest_id, start_date, end_date FROM contests ORDER BY start_date

  $get_contests->execute;

  while (my $contest = $get_contests->fetchrow_hashref) {
    for (@$contest{qw(start_date end_date)}) {
      $_ = DateTime->from_epoch(epoch => $_) if defined $_;
    }

    $contest->{seinfeld} = DateTimeX::Seinfeld->new(
      start_date => $contest->{start_date},
      increment  => { weeks => 1 },
    );

    push @contests, $contest;
  }
} # end load_contests

#---------------------------------------------------------------------
sub process_authors
{
  my ($author_nums, $continued) = @_;

  local $update_standings = $db->prepare(<<'');
INSERT OR REPLACE INTO standings
  (longest_start, longest_length,
   last_start, last_end, last_length,
   active_weeks, total_releases,
   author_num, contest_id)
VALUES (?,?,?,?,?,?,?,?,?)

  local $update_last_release = $db->prepare(<<'');
UPDATE authors SET last_release = ? WHERE author_num = ?

  local $delete_standings;
  $delete_standings = $db->prepare(<<'') unless $continued;
DELETE FROM standings WHERE author_num = ? AND contest_id = ?

  local $get_releases =
      $db->prepare(sprintf <<'', ($continued ? 'AND date > ?' : ''));
SELECT date FROM releases WHERE author_num = ? %s ORDER BY date

  local $get_previous_chains;
  $get_previous_chains = $db->prepare(<<'') if $continued;
SELECT longest_start, longest_length,
  last_start, last_end, last_length,
  active_weeks, total_releases
  FROM standings WHERE author_num = ? AND contest_id = ?

  local $get_last_release;
  $get_last_release = $db->prepare(<<'') if $continued;
SELECT last_release FROM authors WHERE author_num = ?

  my $updated;
  my $count;
  foreach my $aNum (@$author_nums) {
    my @last_release;
    @last_release = $db->selectrow_array($get_last_release, undef, $aNum) // 0
        if $get_last_release;

    my $dates = $db->selectcol_arrayref($get_releases, undef,
                                        $aNum, @last_release);
    next unless @$dates or not $continued;
    my $new_release = $dates->[-1];

    for (@$dates, @last_release) {
      $_ = DateTime->from_epoch(epoch => $_) if defined $_;
    }

    foreach my $contest (@contests) {
      update_author_in_contest($aNum, $contest, $dates, $last_release[0]);
    }

    ++$updated;
    $update_last_release->execute($new_release, $aNum);

    if (++$count > 99) {
      say $aNum unless $continued;
      $db->commit;
      $count = 0;
    }
  } # end foreach $aNum

  $db->commit;

  return $updated;
} # end process_authors

#---------------------------------------------------------------------
sub update_author_in_contest
{
  my ($aNum, $contest, $dates, $last_release) = @_;

  # Trim dates before contest start:
  #   (Contests must be sorted by start date to avoid missing data)
  my $contest_start = $contest->{start_date};

  shift @$dates while @$dates and $dates->[0] < $contest_start;

  # Trim dates after contest end:
  my $contest_end = $contest->{end_date};

  if (@$dates and $contest_end and $dates->[-1] >= $contest_end) {
    $dates = [ @$dates ];  # clone array because we're discarding data

    pop @$dates while @$dates and $dates->[-1] >= $contest_end;
  } # end if releases after contest_end

  if (@$dates) {
    my ($prev, $info);

    if ($get_previous_chains) {
      $prev = $db->selectrow_hashref($get_previous_chains, undef,
                                     $aNum, $contest->{contest_id});
      if ($prev->{last_length}) {
        for (@$prev{qw(longest_start last_start last_end)}) {
          $_ = DateTime->from_epoch(epoch => $_) if defined $_;
        }

        $info = {
          longest => {
            start_period => $prev->{longest_start},
            length       => $prev->{longest_length},
          },
          last    => {
            start_period => $prev->{last_start},
            end_period   => $prev->{last_end},
            end_event    => $last_release,
            length       => $prev->{last_length},
          },
          marked_periods => $prev->{active_weeks},
        };
      } # end if author has existing chains
    } # end if continuing previous calculations

    $info = $contest->{seinfeld}->find_chains( $dates, $info );

    $update_standings->execute(
      $info->{longest}{start_period}->epoch, $info->{longest}{length},
      $info->{last}{start_period}->epoch, $info->{last}{end_period}->epoch,
      $info->{last}{length},
      $info->{marked_periods}, @$dates + ($prev->{total_releases} // 0),
      $aNum, $contest->{contest_id}
    );
  } else {
    $delete_standings->execute($aNum, $contest->{contest_id})
        if $delete_standings;
  }
} # end find_chains

#=====================================================================
# Package Return Value:

1;

__END__
