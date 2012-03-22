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

my %start_time = (
  con  => DateTime->new(qw(year 2012 month 1 day  1 time_zone UTC)),
  hist => DateTime->new(qw(year 1995 month 8 day 13 time_zone UTC)),
);

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
    # If an author's longest chain is longer than his current chain,
    # both chains are eligible for the all_time list.  Otherwise, only
    # the current chain is considered.  This allows authors with a
    # record chain to see how their current effort is going.
    return <<"";
SELECT
  author_id AS id,
  ${type}_last_start AS start,
  ${type}_last_length AS length,
  ${type}_active_weeks AS active_weeks,
  (${type}_last_end = $curPeriod) AS endangered,
  (${type}_last_end >= $curPeriod) AS ongoing
FROM authors
WHERE ${type}_last_length > 1
UNION
SELECT
  author_id AS id,
  ${type}_longest_start AS start,
  ${type}_longest_length AS length,
  ${type}_active_weeks AS active_weeks,
  0 AS endangered,
  0 AS ongoing
FROM authors
WHERE ${type}_longest_length > ${type}_last_length
$order_by

  }, # end all_time

  current => sub {
  my $type = shift;
  return <<"";
SELECT
  author_id AS id,
  ${type}_last_start AS start,
  ${type}_last_length AS length,
  ${type}_active_weeks AS active_weeks,
  (${type}_last_end = $curPeriod) AS endangered
FROM authors
WHERE ${type}_last_end >= $curPeriod AND ${type}_last_length > 1
$order_by

  }, # end current
); # end %query_creator

sub begin_query
{
  my ($type, $query, $limit) = @_;

  my $total_weeks = $current_period->delta_days( $start_time{$type} )
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
  all_time   => begin_query(qw(con  all_time), 10),
  current    => begin_query(qw(con  current),  10),
  historical => begin_query(qw(hist all_time), 10),
});

#---------------------------------------------------------------------
page('longest.html', { all_time => begin_query(qw(con all_time), 200) });

#---------------------------------------------------------------------
page('current.html', { current => begin_query(qw(con current), 0) });

#---------------------------------------------------------------------
page('historical.html', { all_time => begin_query(qw(hist all_time), 200) });

#---------------------------------------------------------------------
$db->disconnect;

# Local Variables:
# compile-command: "perl makeweb.pl"
# End:
