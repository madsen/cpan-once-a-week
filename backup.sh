#! /bin/sh
#---------------------------------------------------------------------
# Back up the release data to SQL insert statements
#
# Useful for loading it into a revised schema
#---------------------------------------------------------------------

echo '
.mode insert authors
SELECT author_num, author_id FROM authors ORDER BY author_num;
.mode insert dists
SELECT dist_id, dist_name FROM dists ORDER BY dist_id;
.mode insert releases
SELECT author_num, dist_id, version, date FROM releases ORDER BY release_id;
' | sqlite3 seinfeld.db \
  | perl -pi -E '
BEGIN {
  say "BEGIN;";
  %table = (
    authors  => "author_num, author_id",
    dists    => "dist_id, dist_name",
    releases => "author_num, dist_id, version, date",
  )
}
END { say "COMMIT;" }
s/(INSERT INTO (\S+))/$1 ($table{$2})/ ' >backup.sql
