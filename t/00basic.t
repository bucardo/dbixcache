#!perl

## Simply test that we can load the DBIx::Cache module
## and that it gives a good version number

use strict;
use warnings;
use Test::More tests => 2;
select(($|=1,select(STDERR),$|=1)[1]);

BEGIN {
	use_ok('DBIx::Cache') or BAIL_OUT 'Cannot continue without DBIx::Cache';
}
use DBIx::Cache;
like( $DBIx::Cache::VERSION, qr/^v?\d+\.\d+\.\d+(?:_\d+)?$/, qq{Found DBIx::Cache::VERSION as "$DBIx::Cache::VERSION"});


