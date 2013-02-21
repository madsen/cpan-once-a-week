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

use FindBin ();
use lib $FindBin::Bin;

use Contest_Util;
use ElasticSearch ();

#---------------------------------------------------------------------
my $size = 100;
my $es = ElasticSearch->new(
  servers      => 'api.metacpan.org:80',
  transport    => 'httptiny',
  max_requests => 0,
  no_refresh   => 1,
);

#---------------------------------------------------------------------
db_init();

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

my $scroller = $es->scrolled_search(
  index  => 'v0',
  type   => 'release',
  query  => { range => { date => { gte => $date } } },
  scroll => '2h',
  size   => $size,
  fields => [qw(author archive date)],
  sort   => [ { "date" => "asc" } ],
);

while (my @hits = $scroller->next($size)) {
  foreach my $hit (@hits) {
    my $field = $hit->{fields};

    $field->{archive} =~ s!^.*/!!; # remove directories

    ### say "@$field{qw(date author archive)}";

    $addRelease->execute( getID(@author, $field->{author}),
                          @$field{qw(archive date)} );
  } # end foreach $hit in @hits

  $db->commit;

  sleep 2 unless $scroller->eof;
} # end while hits

#---------------------------------------------------------------------
# Update chains for authors with new releases:

load_contests();

my $updated = process_authors( [ values %author ], 1);

exit( $updated ? 0 : 212 );
