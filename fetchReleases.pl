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

use DBI ();
use JSON qw(decode_json);
use LWP::UserAgent ();

#---------------------------------------------------------------------
my $maxHits = 100;
my $url = "http://api.metacpan.org/v0/release/_search?size=$maxHits";

my $queryFormat = <<'END QUERY';
{
  "sort": [ { "release.date" : "asc" } ],
  "fields": ["release.author", "release.distribution", "release.date",
             "release.version_numified"],
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

my $getDist = $db->prepare(<<'');
SELECT dist_id FROM dists WHERE dist_name = ?

my $addDist = $db->prepare(<<'');
INSERT INTO dists (dist_name) VALUES (?)

my $addRelease = $db->prepare(<<'');
INSERT OR IGNORE INTO releases
(author_num, dist_id, version, date) VALUES (?,?,?, strftime('%s', ?))

my (%author, %dist);
my @author = (\%author, $getAuthor, $addAuthor);
my @dist   = (\%dist,   $getDist,   $addDist);

#---------------------------------------------------------------------
my $ua = LWP::UserAgent->new;

my $date = '2012-01-01T00:00:00.000Z';

for (;;) {
  my $r = $ua->post($url, 'Content-Type' => 'application/json; charset=UTF-8',
                    Content => sprintf $queryFormat, $date);

  die $r->status_line unless $r->is_success;

  my $json = decode_json( $r->decoded_content );

  my $hits = $json->{hits}{hits};

  foreach my $hit (@$hits) {
    my $field = $hit->{fields};

    $date = $field->{date};

    $addRelease->execute( getID(@author, $field->{author}),
                          getID(@dist,   $field->{distribution}),
                          @$field{qw(version_numified date)} );
  } # end foreach $hit in @$hits

  $db->commit;

  last if @$hits < $maxHits;
} # end forever
