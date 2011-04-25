# $Id: Makefile.PL,v 1.1 2008/06/03 03:27:47 greg Exp greg $

use ExtUtils::MakeMaker;
use Config;
use strict;
use warnings;
use 5.006001;

## No version.pm for this one, as the prereqs are not loaded yet.
my $VERSION = '1.0.1';

my $HELPMAIL = 'greg@endpoint.com';

my $lib;
BEGIN {
    use vars qw/$sep/;
    my %sep = (
               MacOS   => ':',
               MSWin32 => '\\',
               os2     => '\\',
               VMS     => '\\',
               NetWare => '\\',
               dos     => '\\',
               );
    $sep = $sep{$^O} || '/';
    $lib = join $sep, 't', 'lib';
}

use lib $lib;
if ($VERSION =~ /_/) {
    print "WARNING! This is a test version ($VERSION) and should not be used in production!\n";
}

if (grep { /help/ } @ARGV) {
    print qq{
Usage: perl $0

No other options are necessary, although you may need to
set some evironment variables. See the README file for full details.

The usual steps to install this module:

1.   perl Makefile.PL
2.   make
3.   make test
4.   make install

Do steps 1 to 3 as a normal user, not as root!

If all else fails, email $HELPMAIL for help.

};
    exit 1;

}

print "Configuring DBIx::Cache $VERSION\n";

my %opts =
    (
     NAME           => 'DBIx::Cache',
     VERSION_FROM   => 'Cache.pm',
     INC            => '',
     OBJECT         => '',
     AUTHOR         => 'Greg Sabino Mullane',
     ABSTRACT       => 'Caching for Perl DBI',
     PREREQ_PM      => {
                        'ExtUtils::MakeMaker' => '6.11',
                        'DBI'                 => '1.52',
                        'Digest::SHA'         => 0,
                        'Test::More'          => '0.61',
                        'version'             => '0',
                       },
     PERL_MALLOC_OK => 1,
     NEEDS_LINKING  => 0,
     NO_META        => 1,
     clean          => { FILES => 'Makefile.old' },
);
my $output = WriteMakefile(%opts);

exit 0;

# end of Makefile.PL
