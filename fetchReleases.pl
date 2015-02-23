#! /usr/local/bin/perl
#---------------------------------------------------------------------
# fetchReleases.pl
# Copyright 2012 Christopher J. Madsen
#
# Get release dates from MetaCPAN and store them in the database
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use DateTime ();
use DateTimeX::Seinfeld 0.02 ();
use DBI ();
use File::Temp qw(tempdir);
use Search::Elasticsearch ();

#---------------------------------------------------------------------
# Connect to the main database

my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 0, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

#---------------------------------------------------------------------
# Elasticsearch can't efficiently sort a scrolled search.
# So, we dump the records into a temporary database and sort locally.
# (It's important to insert records by date, because the max date in
# the releases table tells us where to start the download next time.
# If something interrupts the download, we don't want to miss releases.
# Also, this way the author_num column is assigned in the order that
# people first uploaded something to CPAN.)

my $tempdir = tempdir(CLEANUP => 1);

my $tdb = DBI->connect("dbi:SQLite:dbname=$tempdir/temp.db","","",
                       { AutoCommit => 0, PrintError => 0, RaiseError => 1 });

{
  # Create a table in the temporary database
  $tdb->do(<<'');
  CREATE TABLE releases (
    author   TEXT NOT NULL,
    filename TEXT NOT NULL,
    date     TIMESTAMP NOT NULL,
    UNIQUE(author, filename)
  )

  my $addRelease = $tdb->prepare(<<'');
  INSERT OR IGNORE INTO releases
  (author, filename, date) VALUES (?,?, strftime('%s', ?))

  my $size = 100;               # records per Elasticsearch batch

  # Figure out where we need to start fetching new releases
  my $date = $db->selectrow_array('SELECT MAX(date) FROM releases');
  if ($date) {
    # subtract 15 minutes just in case something got added out of order
    $date = DateTime->from_epoch(epoch => $date)
                    ->subtract( minutes => 15 )
                    ->iso8601 . 'Z';
  } else {
    $date = '1995-08-16T00:00:00.000Z'; # Happy birthday, CPAN!
  }

  # Start the scrolling query on MetaCPAN
  my $es = Search::Elasticsearch->new(
    cxn_pool => 'Static::NoPing',
    nodes    => 'api.metacpan.org:80',
  );

  my $scroller = $es->scroll_helper(
    index       => 'v0',
    type        => 'release',
    search_type => 'scan',
    scroll      => '2m',
    size        => $size,
    body        => {
      fields => [qw(author archive date)],
      query  => { range => { date => { gte => $date } } },
    },
  );

  # Insert records into the temporary database
  while (my @hits = $scroller->next($size)) {
    foreach my $hit (@hits) {
      my $field = $hit->{fields};

      $field->{archive} =~ s!^.*/!!; # remove directories

      ### say "@$field{qw(date author archive)}";

      $addRelease->execute( @$field{qw(author archive date)} );
    } # end foreach $hit in @hits

    $tdb->commit;

    sleep 2 unless $scroller->is_finished;
  } # end while hits
}

#---------------------------------------------------------------------
# Prepare to transfer the records into the main db
#---------------------------------------------------------------------

sub getID
{
  my ($cache, $get, $add, $name) = @_;

  $cache->{$name} //= do {
    my $id;
    while (not defined( $id = $db->selectrow_array($get, undef, $name) )) {
      $add->execute( $name );
    }

    $id;
  };
} # end getID

#---------------------------------------------------------------------
my $getAuthor = $db->prepare(<<'');
SELECT author_num FROM authors WHERE author_id = ?

my $addAuthor = $db->prepare(<<'');
INSERT INTO authors (author_id) VALUES (?)

my $addRelease = $db->prepare(<<'');
INSERT OR IGNORE INTO releases
(author_num, filename, date) VALUES (?,?, ?)

my (%author);
my @author = (\%author, $getAuthor, $addAuthor);

#---------------------------------------------------------------------
# Transfer release data from temporary db to main db:

{
  my $getReleases = $tdb->prepare(<<'');
SELECT author, filename, date FROM releases ORDER BY date

  $getReleases->execute;

  while (my $row = $getReleases->fetchrow_arrayref) {
    $addRelease->execute( getID(@author, $row->[0]), @$row[1,2] );
  } # end while hits

  $tdb->disconnect;
}

#---------------------------------------------------------------------
# Update chains for authors with new releases:

my $getReleases = $db->prepare(<<'');
SELECT date FROM releases WHERE author_num = ? AND date > ? ORDER BY date

my $getAuthorInfo = $db->prepare(<<'');
SELECT con_longest_start, con_longest_length,
  con_last_start, con_last_end, con_last_length,
  con_active_weeks, con_total_releases,
  hist_longest_start, hist_longest_length,
  hist_last_start, hist_last_end, hist_last_length,
  hist_active_weeks, hist_total_releases,
  last_release
  FROM authors WHERE author_num = ?

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

my %seinfeld = (
  con => DateTimeX::Seinfeld->new(
    start_date => {qw(year 2012 month 1 day 1 time_zone UTC)},
    increment  => { weeks => 1 },
  ),

  hist => DateTimeX::Seinfeld->new(
    start_date => {qw(year 1995 month 8 day 13 time_zone UTC)},
    increment  => { weeks => 1 },
  ),
);

my $contest_start = $seinfeld{con}->start_date;
my $updated;

for my $aNum (values %author) {
  my $author = $db->selectrow_hashref($getAuthorInfo, undef, $aNum);

  my $dates = $db->selectcol_arrayref($getReleases, undef,
                                      $aNum, $author->{last_release} // 0);

  next unless @$dates;

  for (@$dates,
       @$author{qw(con_longest_start con_last_start con_last_end
                   hist_longest_start hist_last_start hist_last_end
                   last_release)}) {
    $_ = DateTime->from_epoch(epoch => $_) if defined $_;
  }

  my @values;
  for my $type (qw(con hist)) {
    my $info;
    if ($author->{"${type}_last_length"}) {
      $info = {
        longest => {
          start_period => $author->{"${type}_longest_start"},
          length       => $author->{"${type}_longest_length"},
        },
        last    => {
          start_period => $author->{"${type}_last_start"},
          end_period   => $author->{"${type}_last_end"},
          end_event    => $author->{last_release},
          length       => $author->{"${type}_last_length"},
        },
        marked_periods => $author->{"${type}_active_weeks"},
      };
    } # end if author has existing chains

    $info = $seinfeld{$type}->find_chains( $dates, $info );

    push @values,
      $info->{longest}{start_period}->epoch, $info->{longest}{length},
      $info->{last}{start_period}->epoch, $info->{last}{end_period}->epoch,
      $info->{last}{length},
      $info->{marked_periods}, $author->{"${type}_total_releases"} + @$dates;

    push @values, $info->{last}{end_event}->epoch if $type eq 'hist';
  } # end foreach $type

  ++$updated;
  $update->execute(@values, $aNum);
} # end for each $aNum in %author

$db->commit;
$db->disconnect;

exit( $updated ? 0 : 212 );
