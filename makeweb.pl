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
$db->{FetchHashKeyName} = 'NAME_lc';

my $tt = Template->new({
  INCLUDE_PATH => "$Bin/templates",
  OUTPUT_PATH  => "$Bin/web",
  EVAL_PERL    => 1,
  TAG_STYLE    => 'asp',
  POST_CHOMP   => 1,
});

# Read information about contests
my $contests = $db->selectall_hashref('SELECT * FROM contests', 'contest_name');

foreach my $contest (values %$contests) {
  for (@$contest{qw(start_date end_date)}) {
    $_ = DateTime->from_epoch(epoch => $_) if defined $_;
  }
}

# Find the beginning of the current period (midnight UTC Sunday):
my $today = DateTime->today;

my $current_period = $today->clone->subtract(days => ($today->day_of_week % 7));

my $curPeriod = $current_period->epoch;

my @endangered_classes = (
  ('')  x 3, # Sun - Tue: no indicator
  ('y') x 2, # Wed - Thu: less than 96 hours to make a release = yellow
  ('z') x 2  # Fri - Sat: less than 48 hours to make a release = red
);
my $endangered_class = $endangered_classes[ $today->day_of_week % 7 ];

chomp(my $order_by = <<'');
ORDER BY
  length DESC,
  active_weeks DESC,
  author_id ASC

my %query_creator = (
  all_time => sub {
    my $type = shift;
    # If an author has a chain under construction (a "current chain"):
    #   If an author's longest chain is longer than his current chain,
    #   both chains are eligible for the all_time list.  Otherwise,
    #   only the current chain is considered.  This allows authors
    #   with a record chain to see how their current effort is going.
    #
    # Else for authors without a current chain:
    #   Only the longest chain is considered.
    return <<"";
SELECT
  author_id AS id,
  last_start AS start,
  last_length AS length,
  active_weeks AS active_weeks,
  (last_end = $curPeriod) AS endangered,
  1 AS ongoing
FROM standings NATURAL LEFT JOIN authors
WHERE contest_id = $contests->{$type}{contest_id}
  AND last_length > 1 AND last_end >= $curPeriod
UNION
SELECT
  author_id AS id,
  longest_start AS start,
  longest_length AS length,
  active_weeks AS active_weeks,
  0 AS endangered,
  0 AS ongoing
FROM standings NATURAL LEFT JOIN authors
WHERE contest_id = $contests->{$type}{contest_id}
  AND (longest_length > last_length
       OR (longest_length > 1 AND last_end < $curPeriod))
$order_by

  }, # end all_time

  current => sub {
  my $type = shift;
  return <<"";
SELECT
  author_id AS id,
  last_start AS start,
  last_length AS length,
  active_weeks AS active_weeks,
  (last_end = $curPeriod) AS endangered
FROM standings NATURAL LEFT JOIN authors
WHERE contest_id = $contests->{$type}{contest_id}
  AND last_end >= $curPeriod AND last_length > 1
$order_by

  }, # end current
); # end %query_creator

sub begin_query
{
  my ($type, $query, $limit) = @_;

  my $total_weeks = $current_period->delta_days($contests->{$type}{start_date})
                                   ->in_units('weeks') + 1;
  my $one_week_percentage = 100 / $total_weeks;

  my (%id_used, %row, $rank, $last_score, $new_score, $d);
  my $s = $db->prepare($query_creator{$query}->($type));
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

    $row{row_id} = do {
      my $id = "id$row{id}";

      die "ID $row{id} already seen twice"
        if $id_used{$id}++ and $id_used{$id .= '.c'}++;

      $id;
    };

    $row{endangered} = '' unless $endangered_class;
    $row{percentage} = sprintf('%.0f%%',
                               $row{active_weeks} * $one_week_percentage);

    $d = DateTime->from_epoch( epoch => $row{start} );
    $row{start_date} = sprintf '%s %d, %d', $d->month_abbr, $d->day, $d->year;

    return \%row;
  };
} # end begin_query

#---------------------------------------------------------------------
sub page
{
  my ($fn, $data) = @_;

  $data->{endangered}  = $endangered_class;

  $tt->process($fn, $data, $fn);
} # end page

#=====================================================================
page('index.html' => {
  all_time   => begin_query(qw(2012 all_time), 10),
  current    => begin_query(qw(2012 current),  10),
  historical => begin_query('All Time', 'all_time', 10),
});

#---------------------------------------------------------------------
page('longest.html', { all_time => begin_query(qw(2012 all_time), 200) });

#---------------------------------------------------------------------
page('current.html', { current => begin_query(qw(2012 current), 0) });

#---------------------------------------------------------------------
page('historical.html', { all_time => begin_query('All Time', 'all_time', 200) });

#---------------------------------------------------------------------
$db->disconnect;

# Local Variables:
# compile-command: "perl makeweb.pl"
# End:
