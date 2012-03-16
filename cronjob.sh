#! /bin/dash

# This provides headers to a wrapper script I use for some cronjobs.
# It sends an email if the job exits non-zero, and otherwise discards
# all output.
cat <<EOF
From: "CPAN Once a Week" <>
Subject: Problem updating website

EOF

MYDIR="$(dirname "$(readlink -f "$0")")"

cd "$MYDIR"

./fetchReleases.pl && \
./makeweb.pl && \
./mirror.sh && \
./backup.pl && \
./data/commit.pl && \
cd data && \
git push github master

exit $?
