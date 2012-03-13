#! /usr/bin/perl
#---------------------------------------------------------------------
# makeweb.pl
# Copyright 2012 Christopher J. Madsen
#
# Generate a website from the Seinfeld chain database
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use FindBin '$Bin';

use DateTime ();
use DBI ();
use Template ();

chdir $Bin or die $!;

my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 1, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

my $tt = Template->new({
  INCLUDE_PATH => "$Bin/templates",
  OUTPUT_PATH  => "$Bin/web",
  EVAL_PERL    => 1,
  TAG_STYLE    => 'asp',
  POST_CHOMP   => 1,
});

my $contestStart = DateTime->new(qw(year 2012 month 1 day 1 time_zone UTC));

# Find the beginning of the current period (midnight UTC Sunday):
my $currentPeriod = DateTime->today;
$currentPeriod = $currentPeriod
                   ->subtract(days => ($currentPeriod->day_of_week % 7));

my $total_weeks = $currentPeriod->delta_days( $contestStart )
                                ->in_units('weeks') + 1;
my $one_week_percentage = 100 / $total_weeks;

$currentPeriod = $currentPeriod->epoch;

chomp(my $order_by = <<'');
ORDER BY
  length DESC,
  active_weeks DESC,
  author_id ASC

my $all_time_query = <<"";
SELECT
  author_id AS id,
  longest_start AS start,
  longest_length AS length,
  active_weeks,
  (last_end = $currentPeriod) AS endangered,
  (last_start = longest_start AND last_end >= $currentPeriod) AS ongoing
FROM authors
WHERE longest_length > 1
$order_by

my $current_query = <<"";
SELECT
  author_id AS id,
  last_start AS start,
  last_length AS length,
  active_weeks,
  (last_end = $currentPeriod) AS endangered
FROM authors
WHERE last_end >= $currentPeriod AND last_length > 1
$order_by

sub begin_query
{
  my ($query, $limit) = @_;

  my (%row, $rank, $last_score, $new_score, $d);
  my $s = $db->prepare($query);
  $s->execute;
  $s->bind_columns( \( @row{ @{$s->{NAME_lc} } } ));

  $last_score = '';

  return sub {
    $s->fetch or return undef;

    ++$rank;
    $new_score = "@row{qw(length active_weeks)}";
    $row{ranking} = do {
      if ($new_score eq $last_score) {
        '';                     # We have a tie; hide the rank
      } elsif ($limit and $rank > $limit) {
        $s->finish;
        return undef;           # We hit the limit
      } else {
        $rank;
      }
    };
    $last_score = $new_score;

    $row{percentage} = sprintf('%.0f%%',
                               $row{active_weeks} * $one_week_percentage);

    $d = DateTime->from_epoch( epoch => $row{start} );
    $row{start_date} = sprintf '%s %d, %d', $d->month_abbr, $d->day, $d->year;

    return \%row;
  };
} # end begin_query

#---------------------------------------------------------------------
my $fn   = 'index.html';
my $data = {
  all_time => begin_query($all_time_query, 10),
  current  => begin_query($current_query, 10),
};

$tt->process($fn, $data, $fn);

#---------------------------------------------------------------------
$fn   = 'longest.html';
$data = { all_time => begin_query($all_time_query, 200) };

$tt->process($fn, $data, $fn);

#---------------------------------------------------------------------
$fn   = 'current.html';
$data = { current => begin_query($current_query, 0) };

$tt->process($fn, $data, $fn);

#---------------------------------------------------------------------
$db->disconnect;

# Local Variables:
# compile-command: "perl makeweb.pl"
# End:
