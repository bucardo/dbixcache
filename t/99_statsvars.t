#!perl -- -*-cperl-*-

## Make sure all of our stats in @STATS match with the actual code

use strict;
use warnings;
use Data::Dumper;
use Test::More tests => 1;

my $file = 'Cache.pm';

open my $fh, '<', $file or BAIL_OUT qq{Could not open the file "$file": $!\n};

my %stat;
my $status = 1;

while (<$fh>) {

	if (1== $status) {
		$status = 2 if /^our \@STATS/;
		next;
	}

	if (2 == $status) {
		while (/(\w+)/g) {
			die "Repeated stat: $1\n" if exists $stat{$1};
			$stat{$1} = 0;
		}
		$status = 3 if /;/;
		next;
	}

	if (/->(more|less)(.+)/) {
		my ($action,$string) = ($1,$2);
		while ($string =~ /'(.+?)'/g) {
			my $stat = $1;
			if (!exists $stat{$stat}) {
				fail qq{Unknown stat called via dbh->$action(): "$stat"\n};
			}
			else {
				$stat{$stat}++;
			}
		}
		next;
	}
}
close $fh or warn qq{Could not close "$file": $!\n};

## Have they all been used?
for (sort keys %stat) {
	next if $stat{$_};
	fail qq{Unused stat "$_"\n};
}


pass "Finished parsing $file\n";
