#!perl

## Basic connection tests

use strict;
use warnings;
use Test::More qw/no_plan/;
use Data::Dumper;
use DBIx::Cache;
use lib 't','.';
select(($|=1,select(STDERR),$|=1)[1]);

use vars qw/$t $dbh $sth $info $count $SQL/;

{
	local $ENV{DBI_DRIVER}='';

	$t = q{DBIx::Cache->connect throws a DBI error with invalid arguments};
	eval { $dbh = DBIx::Cache->connect('baddsn'); };
	like($@, qr{DBI_DRIVER}, $t);

}

$t = q{DBIx::Cache->connect fails when given a non-supported DBD};
eval { $dbh = DBIx::Cache->connect('dbi:Sponge:', '', '', { RaiseError => 1 }); };
like($@, qr{DBIx::Cache does not support}, $t);

$t = q{DBIx::Cache->connect works};
eval { $dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1 }); };
if ($@) {
	BAIL_OUT "Call to connect() failed: has DBI_DSN been set properly? Error was: $@\n";
}

$t = q{DBIx::Cache->connect fails when given an invalid caching method};
eval { $dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1, dxc_cachetype => 'bad' }); };
like($@, qr{how to handle a type}, $t);

$t = q{DBIx::Cache->connect sets a default namespace};
$dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1 });
is($dbh->{private_meth}{namespace}, 'joy.', $t);

$t = q{DBIx::Cache->connect does not by default reset stats};
is($dbh->{private_meth}->get('fetch_hit'), undef, $t);
$dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1 });
is($dbh->{private_meth}->get('fetch_hit'), undef, $t);

$t = q{DBIx::Cache->connect with dxc_reset_stats does as expected};
$dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1, dxc_reset_stats => 1 });
is($dbh->{private_meth}->get('fetch_hit'), 0, $t);

$t = q{DBIx::Cache->connect does not by default use stats};
$dbh->prepare('SELECT 1');
is ($dbh->get_dxc_stats()->{prepare_miss}[0], 0, $t);

$t = q{DBIx::Cache->connect with dxc_stats uses stats};
$dbh = DBIx::Cache->connect('', '', '', { RaiseError => 1, dxc_stats => 1 });
$dbh->prepare('SELECT 1');
is ($dbh->get_dxc_stats()->{prepare_miss}[0], 1, $t);

