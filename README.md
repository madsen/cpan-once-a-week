The CPAN “Once a Week, Every Week” Contest
==========================================

This is the Git repository for the [CPAN “Once a Week, Every Week”
Contest website](http://onceaweek.cjmweb.net/).  You can get the release database from
[the data Git repository](https://github.com/madsen/cpan-once-a-week-data)
and store it in the `data/` directory of this repository.  (It's not a
Git submodule because I don't want to link it to any particular revision.)

Recreating the Website
======================

1. Make sure you've checked out the most recent release data into
the `data/` subdirectory.

2. Run the `createDB.pl` script to create `seinfeld.db` and populate
it with the release data.

3. Run the `recalc.pl` script to calculate Seinfeld chains from the
release data.

4. Run the `makeweb.pl` script to process the templates in the
`templates/` directory and create the output files in the `web/`
directory.

Copyright and License
=====================

This software is copyright (c) 2012 by Christopher J. Madsen.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.
