#! /usr/bin/perl
#---------------------------------------------------------------------
# recalc.pl
# Copyright 2012 Christopher J. Madsen
#
# Regenerate the Seinfeld chains from scratch
#---------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use FindBin ();
use lib $FindBin::Bin;

use Contest_Util;

db_init();
load_contests();

my $author_nums = $db->selectcol_arrayref('SELECT author_num FROM authors');

process_authors($author_nums, 0);
