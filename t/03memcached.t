#!perl

## This tests DBIx::Cache using a memcached backend

use strict;
use warnings;
use Test::More;
use Data::Dumper;
use Digest::SHA qw/sha256_hex/;
use DBIx::Cache;
use lib 't','.';
select(($|=1,select(STDERR),$|=1)[1]);

use vars qw/$t $dbh $sth $info $count $SQL $key/;

our $ADDRESS = 'localhost:11211';

eval { require Cache::Memcached::Fast; };
if ($@) {
	plan skip_all => q{Cannot test unless the Perl module 'Cache::Memcached::Fast' is installed};
}
else {
	require 'dbixcache.setup' or die $@;
	plan tests => number_of_tests() + 3;
}

my $table = test_table();

my $mc;
eval {
	$mc = Cache::Memcached::Fast->new
		(
		 {
		  servers => [ { address => $ADDRESS, weight => 2.5 },
					   ],
		  namespace => 'joy',
		  }
);
};
if ($@) {
	fail 'Cannot test without a memcached connection!';
	exit;
}

$t=q{Connection to DBIx::Cache works};
eval { $dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1, dxc_cachehandle => $mc }); };
is($@, q{}, $t);

$@ and BAIL_OUT "Cannot continue testing if no DBIx::Cache object\n";
$dbh->{PrintError} = 0;
$dbh->{AutoCommit} = 0;

run_standard_tests($dbh);

