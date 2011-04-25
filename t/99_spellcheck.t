#!perl

## Spellcheck as much as we can
## Requires TEST_SPELL to be set

use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

my (@testfiles, $fh);

if (!$ENV{TEST_SPELL}) {
	plan skip_all => 'Set the environment variable TEST_SPELL to enable this test';
}
elsif (!eval { require Text::SpellChecker; 1 }) {
	plan skip_all => 'Could not find Text::SpellChecker';
}
else {
	opendir my $dir, 't' or die qq{Could not open directory 't': $!\n};
	@testfiles = map { "t/$_" } grep { /^.+\.(t|pl)$/ } readdir $dir;
	closedir $dir or die qq{Could not closedir "$dir": $!\n};
	plan tests => 4+@testfiles;
}

my %okword;
my $file = 'Common';
while (<DATA>) {
	if (/^## (.+):/) {
		$file = $1;
		next;
	}
	next if /^#/ or ! /\w/;
	for (split) {
		$okword{$file}{$_}++;
	}
}


sub spellcheck {
	my ($desc, $text, $filename) = @_;
	my $check = Text::SpellChecker->new(text => $text);
	my %badword;
	while (my $word = $check->next_word) {
		next if $okword{Common}{$word} or $okword{$filename}{$word};
		$badword{$word}++;
	}
	my $count = keys %badword;
	if (! $count) {
		pass("Spell check passed for $desc");
		return;
	}
	fail ("Spell check failed for $desc. Bad words: $count");
	for (sort keys %badword) {
		diag "$_\n";
	}
	return;
}


## First, the plain textfiles
for my $file (qw/README Changes/) {
	if (!open $fh, '<', $file) {
		fail(qq{Could not find the file "$file"!});
	}
	else {
		{ local $/; $_ = <$fh>; }
		close $fh or warn qq{Could not close "$file": $!\n};
		if ($file eq 'Changes') {
			s{\b(?:from|by) [A-Z][\w \.]+[<\[\n]}{}gs;
			s{\b[Tt]hanks to ([A-Z]\w+\W){1,3}}{}gs;
			s{Abhijit Menon-Sen}{}gs;
			s{eg/lotest.pl}{};
			s{\[.+?\]}{}gs;
			s{\S+\@\S+\.\S+}{}gs;
		}
		elsif ($file eq 'README.dev') {
			s/^\t\$.+//gsm;
		}
		spellcheck($file => $_, $file);
	}
}

## Now the embedded POD
SKIP: {
	if (!eval { require Pod::Spell; 1 }) {
		skip 'Need Pod::Spell to test the spelling of embedded POD', 2;
	}

	for my $file (qw{Cache.pm}) {
		if (! -e $file) {
			fail(qq{Could not find the file "$file"!});
		}
		my $string = qx{podspell $file};
		spellcheck("POD from $file" => $string, $file);
	}
}

## Now the comments
SKIP: {
	if (!eval { require File::Comments; 1 }) {
		skip 'Need File::Comments to test the spelling inside comments', 2;
	}

	my $fc = File::Comments->new();

	my @files;
	for (sort @testfiles) {
		push @files, "$_";
	}


	for my $file (@testfiles, qw{Cache.pm}) {
		## Tests as well?
		if (! -e $file) {
			fail(qq{Could not find the file "$file"!});
		}
		my $string = $fc->comments($file);
		if (! $string) {
			fail(qq{Could not get comments from file $file});
			next;
		}
		$string = join "\n" => @$string;
		$string =~ s/=head1.+//sm;
		spellcheck("comments from $file" => $string, $file);
	}


}


__DATA__
## These words are okay

## Common:

DBIx
perl
backend
memcached
dbi
cpansign
BDB
username
YAML
Spellcheck
cperl
CPAN
DBD
DSN
Makefile
PGPORT
bucardo
bugzilla
RaiseError
cachetype
README
dbixcache
http
env
textfiles
FETCHPATH
arrayref
cacheability
cacheable
dbixclass
decr
dxc
fetchall
fetchrow
hashref
incr
needsexplain
pcc
statsvars
tablelist
usr


## t/99_yaml.t:

yml

