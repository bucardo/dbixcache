#!perl

## This tests DBIx::Cache using a Berkeley DB as the backend via DB_File

use strict;
use warnings;
use Test::More qw/no_plan/;
use Data::Dumper;
use Digest::SHA qw/sha256_hex/;
use DBIx::Cache;
use lib 't','.';
select(($|=1,select(STDERR),$|=1)[1]);

use vars qw/$t $dbh $sth $info $count $SQL $key/;

eval { require DB_File; };
if ($@) {
	plan skip_all => q{Cannot test unless the Perl module 'DB_File' is installed};
}
else {
	#plan tests => 12;
}


## Require master file
## Pick a database and connect to it
## Create dummy files
require 'dbixcache.setup';

my $table = test_table();

$t = q{DBIx::Cache->connect works with no arguments};
eval { $dbh = DBIx::Cache->connect(); };
is($@, q{}, $t);
$@ and BAIL_OUT "Cannot continue testing if no DBIx::Cache object\n";
$dbh->{PrintError} = 0;
$dbh->{AutoCommit} = 0;

my $t = q{Default cache type is 'DB_File'};
is($dbh->{private_dbixc_type}, 'DB_File', $t);

run_standard_tests($dbh);

