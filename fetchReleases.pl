#! /usr/bin/perl
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
use JSON qw(decode_json);
use LWP::UserAgent ();

#---------------------------------------------------------------------
my $maxHits = 100;
my $url = "http://api.metacpan.org/v0/release/_search?size=$maxHits";

my $queryFormat = <<'END QUERY';
{
  "sort": [ { "release.date" : "asc" } ],
  "fields": ["release.author", "release.archive", "release.date"],
  "query": {
    "range" : {
        "release.date" : {
            "from" : "%s"
        }
    }
  }
}
END QUERY

$queryFormat =~ s/\s{2,}/ /g;   # compress it

#---------------------------------------------------------------------
my $db = DBI->connect("dbi:SQLite:dbname=seinfeld.db","","",
                      { AutoCommit => 0, PrintError => 0, RaiseError => 1 });
$db->do("PRAGMA foreign_keys = ON");

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
(author_num, filename, date) VALUES (?,?, strftime('%s', ?))

my (%author);
my @author = (\%author, $getAuthor, $addAuthor);

#---------------------------------------------------------------------
my $ua = LWP::UserAgent->new;

my $date = $db->selectrow_array('SELECT MAX(date) FROM releases');

if ($date) {
  # subtract 15 minutes just in case something got added out of order
  $date = DateTime->from_epoch(epoch => $date)
                  ->subtract( minutes => 15 )
                  ->iso8601 . 'Z';
} else {
  $date = '2012-01-01T00:00:00.000Z';
}

#---------------------------------------------------------------------
# Fetch release data:

for (;;) {
  my $r = $ua->post($url, 'Content-Type' => 'application/json; charset=UTF-8',
                    Content => sprintf $queryFormat, $date);

  die $r->status_line unless $r->is_success;

  my $json = decode_json( $r->decoded_content );

  my $hits = $json->{hits}{hits};

  foreach my $hit (@$hits) {
    my $field = $hit->{fields};

    $date = $field->{date};
    $field->{archive} =~ s!^.*/!!; # remove directories

    $addRelease->execute( getID(@author, $field->{author}),
                          @$field{qw(archive date)} );
  } # end foreach $hit in @$hits

  $db->commit;

  last if @$hits < $maxHits;
} # end forever

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

exit( $updated ? 0 : 212 );
