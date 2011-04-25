#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-
#
# Copyright 2008-2011 Greg Sabino Mullane <greg@endpoint.com>
#
# Please visit http://bucardo.org/dbixclass for more information
#
# You may distribute under the terms of either the GNU General Public
# License or the Artistic License, as specified in the Perl README file.

use strict;
use warnings;
use 5.006001;

our $DEBUG = 0;

our $STATS = 0;

our $CLASHWAIT = '10';

## All out internal stats items
## The test t/99_statsvars.t keeps track of these for us
our %STATS = (
    badexecuteargs            => 'The execute() method was passed the wrong number of arguments',
    execute_block_hit         => 'Internal stat',
    execute_block_miss        => 'Internal stat',
    execute_hit               => 'The execute() method found a match in the cache',
    execute_miss              => 'The execute() method did not find a match in the cache',
    faahit                    => 'Internal stat',
    faamiss                   => 'Internal stat',
    fahr_hit                  => 'Internal stat',
    fahr_miss                 => 'Internal stat',
    fetch                     => 'Internal stat',
    fetch_hit                 => 'Internal stat',
    fetch_miss                => 'Internal stat',
    fetchallarrayref          => 'Internal stat',
    fetchallhashref           => 'Internal stat',
    fetchend                  => 'Internal stat',
    fetchrowarray             => 'Internal stat',
    fetchrowarray_end         => 'Internal stat',
    fetchrowarray_execute     => 'Internal stat',
    fetchrowarrayref          => 'Internal stat',
    fetchrowarrayref_end      => 'Internal stat',
    fetchrowarrayref_execute  => 'Internal stat',
    fetchrowhashref           => 'Internal stat',
    fetchrowhashref_end       => 'Internal stat',
    fetchrowhashref_execute   => 'Internal stat',
    finish_cache              => 'Method finish() was called on a cacheable handle',
    finish_nocache            => 'Method finish() was called on a non-cacheable handle',
    forcecache_hint           => 'Caching was forced via --force_dxc_cache',
    forcenocache_hint         => 'Non-caching was forced via --force_dxc_nocache',
    fra_hit                   => 'Internal stat',
    fra_miss                  => 'Internal stat',
    frar_hit                  => 'Internal stat',
    frar_misst                => 'Internal stat',
    frhr_hit                  => 'Internal stat',
    frhr_miss                 => 'Internal stat',
    needsexplainfail          => 'The execute() method determined the query was NOT cacheable',
    needsexplainok            => 'The execute() method determined the query was cacheable',
    nocache_function          => 'Internal stat',
    nocache_function_force    => 'Internal stat',
    nocache_noselect          => 'Query could not be cached because it did not start with a SELECT',
    nocache_global            => 'Could not cache because global nocache was on',
    nocache_table             => 'Internal stat',
    nocache_table_force       => 'Internal stat',
    noexecute                 => 'The execute() method already knew the query was not cacheable',
    nofetch                   => 'Internal stat',
    nofetchallarrayref        => 'Internal stat',
    nofetchallhashref         => 'Internal stat',
    nofetchrowarray           => 'Internal stat',
    nofetchrowarrayref        => 'Internal stat',
    nofetchrowhashref         => 'Internal stat',
    prepare_hit               => 'The prepare() method found a match in the cache',
    prepare_miss              => 'The prepare() method did not find a match in the cache',
);

## Dynamic keys
## p:{SQL} - Indicates the cacheability of the statement: 1 or 0 or a ref? (private_cacheable = tablelist)
## Store separate?
## pcc:{SQL} - ???
## FAA{FETCHPATH} - ???

## Private vars
## private_cacheable
## private_needsexplain

my $flow = q{

* prepare:

p:SQL is a boolean - cacheable or not? undef = unknown 1 = yes 0 = no
if (defined p:SQL) {
  sth->{private_cacheable} = p:SQL (1 or 0)
}
else {
  sth->{private_needsexplain} = 1;
}
read-only in prepare: actually set in execute

* execute:

* p:SQL - set when known as 1 or 0 (write) [tablelist]
* e:SQL{args} undef - dunno, defined = #rows:fetchpath (read/write) [tablelist]
* pcc:SQL{args} - stake a claim for this query (read/write) [deletes]
* p:SQL:tables {private_tablelist} stores the tablelist (write)
* private_lastexecutelive - execute was real, not cached

* store_key:

T:{schema.table} - hash of queries that use this table

* pg_parse_affected_tables:

oid2relnames = {private_oid2relanames} - hash mapping oids to [schema, relname, kind]
volfuncs = {private_volfuncs} - hash mapping oids to volatility flag
returns \@tables : [schema, relname, kind]

* fetch

We know that execute *must* have been run. This gets us:
1) explain has been run, needsexplain always cleared
2) private_cacheable is authoritative
3) list of tables is available, as either private or key

F{rownum}:{fetchpath} - actual row information. (read/write) [tablelist]
Fetchpath means 'this particular execute'
Tables are fetched from private_tablelist || fetch_key(p:SQL:tables)

* fetchall_arrayref

Each fetchpath can only go to a single type of returned info.

FAA{fetchpath}{args}

If we get a miss, check private_lastexecutelive. If false, 
re-run the execute and set private_lastexecutelive to true.


};



## This must be unique for us. Keep it simple - letters only.
our $NAMESPACE = 'joy.';

## database and lowercase database names
our $db = '?';
our $ldb = '?';

#######################################
package DBIx::Cache;

use DBI 1.49;
use base         qw{ DBI    };
use Data::Dumper qw{ Dumper };

our $VERSION = '1.0.1';

## How long to keep expired fetch information around, in seconds
## This should be longer than your longest execute() -> finish() path.
my $FETCH_EXPIRETIME = 60 * 10;

my $DEFAULT_CACHETYPE = 'DB_File';

## The minimum version we support for each DBD
my %MINDBD = (
              Pg => version->new('2.4.0'),
);

sub new {
    die qq{Do not use 'new', use 'connect': see the DBIx::Cache documentation\n};
}

sub connect {

    ## The main entry to the DBIx::Cache module. Use in place of DBI->connect

    my $self = shift;

    ## Call the parent DBI, die right away if it fails
    my $dbh = $self->SUPER::connect(@_) or die;

    ## Verify that we can use this particular driver and version
    $db  = $dbh->{Driver}{Name} or die "No driver name found\n";
    if (!exists $MINDBD{$db}) {
        die qq{Sorry, DBIx::Cache does not support DBD::$db\n};
    }
    my $dbdver = $dbh->{Driver}{Version} or die qq{DBD::$db did not provide a Version!\n};
    my $minver = $MINDBD{$db};
    if ($dbdver < $minver) {
        die qq{Sorry, DBIx::Cache requires DBD::$db to be at least version $minver, you have $dbdver\n};
    }

    ## Lowercase version of the driver name to make things easier later on
    $ldb = lc $db;

    ## Always force RaiseError on
    $dbh->{RaiseError} = 1;

    ## What type of caching are we using?
    ## Argument trumps ENV trumps default
    my $attr = $_[3] || {};
    my $cachetype = $dbh->{private_dbixc_type} = $attr->{dxc_cachetype}
        || $ENV{DBIXC_CACHETYPE}
        || $DEFAULT_CACHETYPE;
    $DEBUG and warn "Set cache type to $cachetype\n";

    ## Were we passed in a cachehandle?
    if ($attr->{dxc_cachehandle}) {
        $dbh->{private_meth} = $attr->{dxc_cachehandle};
        $DEBUG >= 1 and warn "Got a cachehandle of $attr->{dxc_cachehandle}\n";
    }
    else {
        if ($cachetype eq 'DB_File') {
            ## If filename is undef, we use in-memory BDB
            $dbh->{private_meth} = DBIx::Cache::DB_File->new($attr->{dxc_filename});
        }
        else {
            die qq{Don't know how to handle a type of $cachetype\n};
        }
    }

    ## Declare our namespace: this also verfies that private_meth is working
    $dbh->{private_meth}->namespace($NAMESPACE);

    ## If requested, reset all stats to zero (and create if they don't exist)
    if ($attr->{dxc_reset_stats}) {
        $dbh->reset_dxc_stats();
    }

    ## Specifically turn stats on or off
    if (exists $attr->{dxc_stats}) {
        $STATS = $attr->{dxc_stats};
    }

    ## No point in doing this test more than a few times
    if (exists $attr->{dxc_no_test}) {
        return $dbh;
    }

    ## Until told otherwise, we're going to cache!
    $dbh->{private_cache} = 1;

    ## Quick sanity check of our caching engine
    my $result;
    eval {
        my $dummy = 'foobar_deleteme';
        $dbh->{private_meth}->set($dummy => 42);
        $result = $dbh->{private_meth}->get($dummy);
        $dbh->{private_meth}->delete($dummy);
    };
    $@ and die "Caching engine failed basic test: $@\n";
    $result == 42 or die "Caching engine failed basic set/get test: set 42, got $result\n";

    return $dbh;

} ## end of connect


#######################################
package DBIx::Cache::dr;
use DBI;
use base qw/DBI::dr/;


#######################################
package DBIx::Cache::db;
use strict;
use warnings;
use DBI;
use base         qw{ DBI::db    };
use Digest::SHA  qw{ sha256_hex };
use Data::Dumper qw{ Dumper     };

sub dxc_cache {
    ## Stop caching altogether
    my $self = shift;
    $self->{private_cache} = shift;
    return;
}

sub reset_dxc_stats {

    ## Reset all the internal statistics to zero

    my $self = shift;
    my $meth = $self->{private_meth};

    return $meth->set_multi(map { [$_ => 0] } keys %STATS);

} ## end of reset_dxc_stats

sub dxc_stats {

    ## Toggle the internal stats gathering on or off

    my ($self,$value) = @_;
    $STATS = $value;

    return !$STATS;

} ## end of dxc_stats


sub get_dxc_stats {

    # Get all internal stats, return in a hashref

    my $self = shift;
    my $meth = $self->{private_meth};

    my $stats = $meth->get_multi(sort keys %STATS); ## XXX Remove sort at end
    my $hashref;
    for (keys %STATS) {
        $hashref->{$_} = [$stats->{$_},$STATS{$_}];
    }
    return $hashref;

} ## end of get_dxc_stats


sub fetch_key {

    ## Given the name of a key, fetch it's value from the cache.
    ## Returns the value of the key, or undef if there is no matching key.
    ## If the key name starts in uppercase, it must be a reference
    ##
    ## Optional arguments (as a hashref):
    ##  - nohash: Do not hash the key (rare)

    my ($self,$key,$opt) = @_;
    my $okey = $key;
    ##warn "FETCHING: $key\n";

    ## We almost always want to hash it
    $key = sha256_hex($key) unless exists $opt->{nohash};

    ## References have a special prefix:
    $key = '@'.$key if $okey =~ /^[A-Z]/o;

    ## Attempt the actual fetch:
    my $meth = $self->{private_meth};
    my $rv = $meth->get($key);

    if ($DEBUG >= 2) {
        my $msg = sprintf "  fetch_key ($okey) => ($key) Result: (%s)", defined $rv ? $rv : 'UNDEF';
        warn "$msg\n";
    }

    ## If recording stats, increment 'hit' and 'miss' but not for 'hit' and 'miss'
    #if ($STATS and $okey ne 'hit' and $okey ne 'miss') {
    #    defined $rv ? $meth->incr('hit') : $meth->decr('miss');
    #}

    return $rv;

} ## end of fetch_key


sub store_key {

    ## Given the name of a key and a value, store it in the cache
    ## Returns whatever the final "set" call returns, usually
    ## undef on error, true or false from the caching server
    ##
    ## Optional arguments (as a hashref):
    ##  - nohash: do not hash the key (rare)
    ##  - expire: number of seconds to expire (absolute, not 30-day memcached crap)
    ##  - tables: an arrayref of tables that can kill this cache
    ##  - ref: this is storing a ref, so prefix the name with a '@'
    ##
    ## Dynamic keys used: T:$tables

    my ($self,$key,$val,$opt) = @_;
    my $okey = $key;

    ## We almost always want to hash it
    $key = sha256_hex($key) unless exists $opt->{nohash};

    ## References have a special prefix:
    $key = '@'.$key if $okey =~ /^[A-Z]/o;

    my $meth = $self->{private_meth};

    my $expire = 0;
    if (exists $opt->{expire}) {
        if ($opt->{expire} !~ /^\d+$/) {
            warn "Invalid expire time passed to store_key: $opt->{expire} (will use 0)\n";
        }
        else {
            $expire = $opt->{expire};
            ## Special adjustment for memcache, which has odd expiration rules
            if (ref $meth =~ /Memcache/ and $expire > 60*60*30) {
                $expire += $^T;
            }
        }
    }

    ## Check for an arrayref of tables that can kill this cache:
    if (exists $opt->{tables} and ref $opt->{tables}) {

        ## We may have passed a statement handle in, in which case grab the stored tablelist
        if (ref $opt->{tables} ne 'ARRAY') {
            my $sth = $opt->{tables};
            my $tablelist = $sth->{private_tablelist};
            if (!defined $tablelist) {
                my $innerkey = 'p:'.$sth->{Statement}.':tables';
                $tablelist = $self->fetch_key($innerkey);
            }
            $opt->{tables} = $tablelist;
        }

        ## Store up all changes so we can do a single store_multi call
        my @multi;

        for my $relname (@{$opt->{tables}}) {
            my $tkey = "T:$relname";

            ## Pull up the current list for this table, or create a new one:
            my $tabcache = $self->fetch_key($tkey) || {};

            ## Skip if this table is already mapped to this (hashed) key:
            next if exists $tabcache->{$key};

            ## If a whereclause was passed in, forward that along as well
            $tabcache->{$key} = $opt->{whereclause} || 1;

            push @multi, [$tkey => $tabcache];
        }

        #@multi and $self->store_multi(\@multi);
    }

    ## Attempt the actual store, *after* we set the cachekiller above:
    my $rv = $meth->set($key,$val,$expire);

    if ($DEBUG >= 2) {
        my $msg = sprintf "  store_key ($okey) => ($key) Result: (%s)", defined $rv ? $rv : 'UNDEF';
        warn "$msg\n";
    }

    return $rv;

} ## end of store_key


## Simple wrapper around incr
sub more {
    my $self = shift;
    return $self->{private_meth}->incr(shift);
}
## Simple wrapper around decr
sub less {
    my $self = shift;
    return $self->{private_meth}->decr(shift);
}


sub delete_key {

    ## Given the name of a key, delete it from the cache
    ## Returns boolean true/flase for server reply, undef on error
    ##
    ## Optional arguments (as a hashref):
    ##  - nohash : do not hash the key (rare)

    my ($self,$key,$opt) = @_;
    my $okey = $key;

    $DEBUG >= 3 and warn "-->Deleting key: $key\n";

    my $meth = $self->{private_meth};

    ## If we are deleting a table cache, delete dependencies at the same time;
    my (@delkeys, @expkeys);
    if ($key =~ /^T:(.+)/) {
        my $cleanup = $self->fetch_key($key);

        ## XXX where clause has changed
        if ($opt->{whereclause}) {
            if (! exists $cleanup->{whereclause}) {
                $DEBUG >= 2 and warn "No whereclause, so skipping\n";
                $STATS and $meth->incr('nowhereclausedelete');
                return;
            }
            if ($cleanup->{whereclause} ne $opt->{whereclause}) {
                $DEBUG >= 2 and warn "Where clauses do not match, skipping\n";
                $STATS and $meth->incr('nowhereclausedeletematch');
                return;
            }
        }

        ## How long does expired fetch info hang around in seconds?
        my $expseconds = $opt->{fetch_expiretime} || $FETCH_EXPIRETIME;

        for my $k (keys %$cleanup) {
            if (index($k,'@')) {
                push @delkeys => $k;
            }
            else {
                my $val = $meth->get($k);
                push @expkeys => [$k,$val,$expseconds];
            }
        }
    }

    ## We almost always want to hash it
    $key = sha256_hex($key) unless exists $opt->{nohash};

    ## References have a special prefix:
    $key = '@'.$key if $okey =~ /^[A-Z]/o;

    my $rv = 1;

    ## Expire fetch keys that may be in use by other processes
    if (@expkeys) {
        $rv = $meth->replace_multi(@expkeys);
        ## XXX $rv may be a hashref, handle it
        if ($DEBUG >= 2) {
            my $msg = sprintf '  EXPIRE FETCHES: got: %s', defined $rv ? $rv : 'UNDEF';
            warn "$msg\n";
        }
    }

    if (@delkeys) {
        ## Delete dependent caches first, then the main one
        $rv = $meth->delete_multi(@delkeys,$key);
    }
    else {
        $rv = $meth->delete($key);
    }
    if ($DEBUG >= 2) {
        my $msg = sprintf "  delete_key ($okey) => ($key) Result: (%s)", defined $rv ? $rv : 'UNDEF';
        warn "$msg\n";
    }

    return $rv;

} ## end of delete_key

sub prepare {

    ## Prepare a database query.
    ## Returns a statement handle, exactly like DBI->prepare does.
    ##
    ## This is where we first attempt to determine if the query is cacheable or not.
    ## If we can tell for sure, we cache that decision.
    ## If not, we defer the decision until the first execute.
    ## Either way, we return the parent's statement handle ($sth)
    ##
    ## Static keys used: nocache_noselect nocache_hint forcecache_hint prepare_hit prepare_miss
    ## Dynamic keys used: p:{SQL}
    ## Private attribs used: cacheable needsexplain whereclause

    my $dbh = shift;

    ## We've forced RaiseError on in the connect already, so this should throw an exception on failure:
    my $sth = $dbh->SUPER::prepare(@_);

    $DEBUG >= 3 and warn "** Start prepare with a sth of $sth\n";
    ## Reset our internal markers:
    $sth->{private_cacheable} = 0;

    ## First cacheable check: is it a SELECT? If not, simply return the handle:
    if ($_[0] !~ /^\s*SELECT/io) {
        $STATS and $dbh->more('nocache_noselect');
        return $sth;
    }

    ## Now check if we should force a cache no matter what. Use with care
    if ($_[0] =~ /--force_dxc_cache/ios) {
        $STATS and $dbh->more('forcecache_hint');
        $sth->{private_forcecache} = 1;
    }
    ## Next check for any hints telling it NOT to cache
    elsif ($_[0] =~ /--force_dxc_nocache/ios) {
        $STATS and $dbh->more('forcenocache_hint');
        $sth->{private_forcenocache} = 1;
        return $sth;
    }

    ## Make sure we are still globally allowed to cache
    if (!$dbh->{private_cache}) {
        $STATS and $dbh->more('nocache_global');
        return $sth;
    }

    ## A specific whereclause matcher, for smarter cache invalidation
    $sth->{private_whereclause} = '';

    ## Check for any args passed in specific to DBIx::Cache
    my $arg = {};

    ## Check if second arg is a hashref - XXX doc better
    if (defined $_[1] and ref $_[1] eq 'HASH') {
        for (grep { /^dxc/ } keys %{$_[1]}) {
            $arg->{$_} = $_[1]->{$_};
            ### All tables vs specific table
            if ($_ eq 'dxc_whereclause') {
                $sth->{private_whereclause} = $arg->{$_};
            }
        }
    }

    ## Do we already know about this query's cacheability?
    my $key = "p:$_[0]";
    my $val = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $val ? 'prepare_hit' : 'prepare_miss');

    if (defined $val) { ## We've seen this before

        $DEBUG >= 2 and warn "PREPARE HIT: $val\n";

        $sth->{private_cacheable} = $val;

    }
    else {

        $DEBUG >= 2 and warn "PREPARE MISSED: $key\n";

        ## We've not seen this query before, so we need to tell execute to 
        ## figure out how cacheable it is the first time it is run
        $sth->{private_needsexplain} = 1;
    }

    ## Always return the statement handle
    return $sth;

} ## end of prepare

sub selectall_arrayref {
    warn "SELECTALLARRAYREF NEEDS FOOD, BADLY!\n";
    my $self = shift;
    ## Cannot call the parent, as we'll get non cache versions of execute and fetch
    return $self->SUPER::selectall_arrayref(@_);
}

sub selectall_hashref {
    die "SELECTALLHASHREF NEEDS FOOD, BADLY!\n";
}

sub selectcol_arrayef {
    die "Incomplete\n";
}
sub selectrow_array {
    die "Incomplete\n";
}
sub selectrow_arrayref {
    die "Incomplete\n";
}
sub selectrow_hashref {
    die "Incomplete\n";
}

sub prepare_cached {
    die "Incomplete\n";
}

## Any method not listed here simply goes to the parent


###########################################
package DBIx::Cache::st;
use strict;
use warnings;
use DBI;
use Time::HiRes  qw{ sleep   };
use base         qw{ DBI::st };
use Data::Dumper qw{ Dumper  };

local $Data::Dumper::Indent=0;
local $Data::Dumper::Varname = '';
local $Data::Dumper::Terse = 1;


sub execute {

    ## Execute a query, return the number of rows affected
    ## Passed in zero or more placeholder values
    ##
    ## If this is cacheable, $sth->{private_cacheable} will be true
    ## (and contain an array of relevant tables)
    ## If we need to figure out its cacheability first,
    ## $sth->{private_needsexplain} will be true
    ##
    ## Static keys used: noexecute badexecuteargs needsexplainfail needsexplainok execute_hit execute_miss
    ##   execute_block_hit execute_block_miss execute_block_timeout
    ## Dynamic keys used: p:{$SQL} pcc:{$SQL} e:{$SQL}{args} pcc:{$SQL}{args}
    ## Private attribs used: needsexplain cacheable preparenum curtuple numrows fetchpath
    ##   executecachehit executeargs whereclause realexecute

    my $sth = shift;

    my $dbh = $sth->{Database};

    $DEBUG >= 3 and warn "** Start execute with a dbh of $dbh\n";

    if (!$sth->{private_needsexplain}) {

        ## We've been here before, so is it cacheable?

        if (!$sth->{private_cacheable}) {
            ## Not cacheable, so pass to the parent and fuggedaboutit
            $DEBUG >= 2 and warn "EXECUTE: NOT CACHEABLE\n";
            $STATS and $dbh->more('noexecute');
            return $sth->SUPER::execute(@_);
        }
    }

    ## Determine if the number of parameters passed in is correct.
    ## If not, we never cache but let the parent handle it
    ## (who will almost certainly throw an error)
    my $total_args    = $sth->{NUM_OF_PARAMS};
    my $bound_args    = $sth->{"${ldb}_numbound"};
    my $actual_args   = @_;

    ## For now, we simply say "all or nothing"
    $DEBUG >= 2 and warn "Execute args decision: (got:$actual_args, total:$total_args, bound: $bound_args)\n";
    if ($actual_args != $total_args or (!$actual_args and $bound_args != $total_args)) {
        $DEBUG and warn "DO NOT CACHE, number of params wrong (got:$actual_args, total:$total_args, bound: $bound_args)\n";
        $STATS and $dbh->more('badexecuteargs');
        return $sth->SUPER::execute(@_);
    }

    ## If this is our first time, we need to get a list of tables and make sure this is a cacheable query
    my $tablelist = [];
    if ($sth->{private_needsexplain}) {

        $DEBUG >= 1 and warn "First call to execute() figures out cacheability\n";

        if ($db ne 'Pg') {
            die "Invalid driver: how did you get here?\n";
        }

        if ($db eq 'Pg') {

            ## If there are no params, we can find the list of tables now
            ## Otherwise, we defer until the first execute when we have valid args

            my $params = $sth->{NUM_OF_PARAMS};

            $dbh->do(q{SAVEPOINT dbix});

            my $parsetree = '';
            {
                my $SQL;

                if (!$params) {
                    ## No params is easy enough
                    $SQL = "EXPLAIN $sth->{Statement}";
                }
                else {
                    my ($i,$sql) = (1,'');
                    $SQL = '';

                    ## Build the query by piecing together segments and placeholders
                    my $seg = $sth->{pg_segments};
                    $sql = join '$'.$i++, @$seg;
                    if ($params >= @$seg) { ## Ends in a placeholder
                        $sql .= "\$$i";
                    }

                    my $num = ++$dbh->{private_preparenum};
                    my $pid = $$;
                    my $prepname = "dbixcache_${pid}_$num";

                    my $rv = $dbh->do("PREPARE $prepname AS $sql")
                        or die qq{Could not prepare query: $sql }.$dbh->errstr.qq{\n};

                    ## We cannot handle any bound_args here, right?
                    ## ParamValues can grab already bound ones
                    ## ParamTypes may come in useful?

                    $SQL = "EXPLAIN EXECUTE $prepname(";
                    $SQL .= join ',' => map { $dbh->quote($_) } @_;
                    $SQL .= ')';

                }

                $dbh->do(q{SET debug_pretty_print = 1});
                $dbh->do(q{SET debug_print_parse = 1});
                local $SIG{__WARN__} = sub { $parsetree .= shift; };
                local $dbh->{PrintWarn} = 1;
                $dbh->do(q{SET client_min_messages = 'DEBUG1'});
                $dbh->do($SQL);
                $dbh->do(q{ROLLBACK TO dbix});
            }

            ## Gather a list of all tables affected, in an arrayref
            $tablelist = _pg_parse_affected_tables($dbh, $sth, $parsetree);

            ## Save this decision, both short and long term:
            my $key = 'p:'.$sth->{Statement};
            $sth->{private_tablelist} = $tablelist;
            $dbh->store_key("$key:tables" => $tablelist);

            ## Map this query to the list of tables it contains,
            ## so invalidation of those tables clears this prepare cache
            $dbh->store_key($key, ref $tablelist ? 1 : 0, { tables => $tablelist } );

            ## If we can't cache it, just call the parent
            if (! ref $tablelist) {
                $DEBUG >= 2 and warn "NEEDSEXPLAIN: NOT CACHEABLE\n";
                $STATS and $dbh->more('needsexplainfail');
                $sth->{private_needsexplain} = 0;
                $sth->{private_cacheable} = 0;
                return $sth->SUPER::execute(@_);
            }
            $sth->{private_cacheable} = 1;

            $STATS and $dbh->more('needsexplainok');

            ## TODO: Consider allowing tables to adjust the expiration time

        } ## end driver eq 'Pg'

        $sth->{private_needsexplain} = 0;

    } ## end of needs_explain

    ## If we are here, the query at least is cacheable

    ## Always reset this to 0: execute cancels all existing fetches
    $sth->{private_curtuple} = 0;

    ## Build the key to see if we've already cached this execute combo
    my $sql = $sth->{Statement};
    my $key = "e:$sql";

    ## Passed in params trump bound ones
    my $vals = $sth->{ParamValues};
    my $NULL = 'N!';
    my $args = '';
    if ($actual_args) {
        for (@_) {
            $args .= defined $_ ? "#$_" : "#$NULL";
        }
    }
    else {
        for (sort keys %$vals) {
            $args .= defined $vals->{$_} ? "#$vals->{$_}" : "#$NULL";
        }
    }
    $key .= $args;
    $DEBUG >=2 and warn "EXECUTE key: $key\n";

    ## Do we have this execute already in the cache?
    my $result = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $result ? 'execute_hit' : 'execute_miss');

    ## We could check private_numrows to catch repeated exec calls,
    ## but then invalidation gets rather tricky.
    ## Maybe add something in later (e.g. for non-expirable entries)

    if (defined $result) {
        RESULT : {
            $DEBUG and warn "EXECUTE HIT: $result\n";

            ## Break the result into number of rows and the pointer
            $result =~ s/(\d+)://o;
            my $rows = $sth->{private_numrows} = $1;

            ## Set our internal marker for subsequent fetches
            $sth->{private_fetchpath} = $result;

            ## We may need to re-execute this if a different fetch is called
            ## e.g. fetchall_arryref vs. fetchall_hashref
            $sth->{private_lastexecutelive} = 0;
            $sth->{private_executeargs} = \@_;
            ## TODO: Handle bind_param

            ## Return the number of rows as if we executed it ourselves
            return $rows;
        }
    }


    ## Have not seen it before, so send off to real execute()
    $DEBUG and print "EXECUTE MISS, running SUPER::execute\n";

    ## We want to avoid a cache clash, so we use a semaphore key rather than waiting for
    ## the real execute to finish
    my $bkey = "pcc:$sth->{Statement}$args";

    ## Loop until it is free, or we time out
    my ($attempts,$starttime) = (0,0);
    my $maxwait = $CLASHWAIT; ## XXX Configurable
  LOOP: {
        ## Is someone else potentially working on it?
        my $val = $dbh->fetch_key($bkey);
        last if ! $val;
        $attempts++;

        ## How long has the other process been at it?
        if (!$starttime and $val =~ /(\d+):(\d+)/o) {
            $starttime = $1;
            $DEBUG >= 2 and warn "Someone else ($1) is working on it (time:$2)\n";
        }
        my $waiting = time() - $starttime;
        $DEBUG >= 2 and warn "Wait time: $waiting\n";

        ## Bail if we've been waiting around too long
        if ($waiting > $maxwait) {
            $DEBUG >= 1 and warn "Timed out, calling execute anyway for $sql\n";
            $STATS and $dbh->more('execute_block_timeout');
            last;
        }

        sleep 0.5;
        redo;
    }

    ## If we were blocked, we should be able to now get a cached version
    ## Even if we timed out, it's worth a final attempt here
    if ($attempts) {
        $result = $dbh->fetch_key($key);
        $STATS and $dbh->more(defined $result ? 'execute_block_hit' : 'execute_block_miss');
        goto RESULT if $result;
    }

    ## Mark this as our property. This key needs no expiration.
    my $now = time();
    $dbh->store_key($bkey, "$$:$now");

    my $rv = $sth->SUPER::execute(@_) or die $sth->errstr;

    ## We need a unique path for the fetches in this particular run of execute()
    my $fetchpath = $^T . rand(1000);

    ## We send the 'tables' option to map this query to each table
    ## for cache invalidation purposes
    ## TODO: Use private_ if available. Shouldn't it always be?
    $dbh->store_key($key, "$rv:$fetchpath", { tables => $tablelist });

    ## Save a local copy of the number of rows
    $sth->{private_numrows} = $rv;

    ## Save a local copy of the fetch path
    $sth->{private_fetchpath} = $fetchpath;

    ## Used to tell if we need to re-run the execute or not
    $sth->{private_lastexecutelive} = 1;

    ## Unmark so others can use this query
    $dbh->delete_key($bkey);

    ## Return the number of rows executed by the parent to the caller
    return $rv;

} ## end of execute

sub execute_array {
    die "Incomplete!\n";
}

sub execute_for_fetch {
    die "Incomplete!\n";
}

sub rows {
    die "What to do?\n";
}
sub bind_col {
    die "Probably will not work at all\n";
}

sub fetch {
    ## Alias to fetchrow_arrayref
    return fetchrow_arrayref(@_);
}

sub fetchrow_arrayref {

    ## Fetch a single row, caching it if needed
    ## Returns the row data, or undef if no more rows
    ##
    ## Static keys used: fetch nofetch fetchend fhit fmiss
    ## Private attribs used: cacheable curtuple numrows whereclause

    my $sth = shift;
    my $dbh = $sth->{Database};

    ## If we are not caching, simply leave it to the parent
    if (! $sth->{private_cacheable}) {
        $STATS and $dbh->more('fetchrowarrayref_nocache');
        return $sth->SUPER::fetch(@_);
    }

    $STATS and $dbh->more('fetchrowarrayref_cache');

    ## If we know there are no more rows, just return an undef
    my $row = $sth->{private_curtuple};
    my $numrows = $sth->{private_numrows}; ## set by execute()
    if ($row >= $numrows) {
        $DEBUG and print "FRAR: No more cached rows, returning undef (row=$row, numrows=$numrows)\n";
        $STATS and $dbh->more('fetchrowarrayref_end');
        $sth->{private_curtuple} = 0;
        return undef;
    }

    ## Attempt to fetch this row from the database
    my $key = "FRAR$row:$sth->{private_fetchpath}";
    $DEBUG >= 1 and warn "FRAR key: $key\n";

    my $hit = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $hit ? 'fetchrowarayref_hit' : 'fetchrowarrayref_miss');

    $sth->{private_curtuple} = $row + 1;

    if (defined $hit) {
        $DEBUG and print "FRAR HIT: $hit\n";
        return $hit;
    }

    ## If numrows > 1, we'll have to pull the other ones. Ugly, but what can you do?
    if (!$sth->{private_lastexecutelive}) {
        my $oldrows = $sth->{private_numrows};
        my $rows = $sth->SUPER::execute(@{$sth->{private_executeargs}});
        if ($rows != $oldrows) {
            warn "DXC Warning: rows have changed from $oldrows to $rows\n";
            $sth->{private_numrows} = $rows;
        }
        $sth->{private_lastexecutelive} = 1;
        ## How many null fetches do we need to do?
        for (1..$row) {
            $sth->SUPER::fetch(@_);
        }
    }

    ## No cached info, so call the parent's fetch
    my $res = $sth->SUPER::fetch(@_);

    ## TODO: Use nowait for here and elsewhere?
    $dbh->store_key($key, $res, { tables => $sth });

    return $res;

} ## end of fetchrow_arrayref


sub fetchall_arrayref {

    ## Return all the rows in a specific format
    ## Return cached version if available
    ## Note: by calling SUPER::fetchall_arrayref, we bypass *our* fetch
    ##
    ## Static keys used: nofetchallarrayref fetchallarrayref faahit faamiss
    ## Dynamic keys used: FAA{fetchpath}
    ## Private attribs used: fetchpath executecachehit executeargs whereclause

    my $sth = shift;

    my $dbh = $sth->{Database};
warn "INSIDE FAA!\n";

    ## If we are not caching, leave it to the parent
    if (! $sth->{private_cacheable}) {
        $STATS and $dbh->more('fetchallarrayref_nocache');
        return $sth->SUPER::fetchall_arrayref(@_);
    }

    $STATS and $dbh->more('fetchallarrayref_cache');

    ## Have we cached this value already? If so, return it
    my $key = "FAA:$sth->{private_fetchpath}";

    ## Since the arguments affect the output, add those in as well
    for (@_) {
        $key .= '#' . Dumper($_);
    }
    $DEBUG >= 2 and print "FETCHALL_ARRAYREF key: $key\n";

    my $hit = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $hit ? 'fetchallarrayref_hit' : 'fetchallarrayref_miss');

    if (defined $hit) {
        $DEBUG and print "FETCHALL_ARRAYREF HIT: $hit\n";
        return $hit;
    }

    ## We need to re-run the execute if it was not just run for real
    if (!$sth->{private_lastexecutelive}) {
        ## This is suboptimal, as we've already returned the number of rows, 
        ## and it may have changed betwen then and now. Best we can do is 
        ## warn people and recommend limiting mix-n-match calls in docs
        my $oldrows = $sth->{private_numrows};
        my $rows = $sth->SUPER::execute(@{$sth->{private_executeargs}});
        if ($rows != $oldrows) {
            warn "DXC Warning: rows have changed from $oldrows to $rows\n";
            $sth->{private_numrows} = $rows;
        }
        $sth->{private_lastexecutelive} = 1;
    }

warn "Calling super version of fetchall_arrayref\n";
    my $res = $sth->SUPER::fetchall_arrayref(@_);

    $dbh->store_key($key, $res, { tables => $sth });

    return $res;

} ## end of fetchall_arrayref


sub fetchrow_array {

    ## Pretty much the same as fetchrow_arrayref
    ##
    ## Static keys used: nofetchrowarray fetchrowarray fetchrowarray_end
    ## Dynamic keys used: FRA{fetchpath}
    ## Private attribs used: cacheable curtuple numrows fetchpath executecachehit executeargs whereclause

    my $sth = shift;
    my $dbh = $sth->{Database};

    ## If we are not caching, leave it to the parent
    if (! $sth->{private_cacheable}) {
        $STATS and $dbh->more('fetchrowarray_nocache');
        return $sth->SUPER::fetchrow_array(@_);
    }

    $STATS and $dbh->more('fetchrowarray_cache');

    ## If we know there are no more rows, just return an undef
    my $row = $sth->{private_curtuple};
    my $numrows = $sth->{private_numrows}; ## set by execute()
    if ($row >= $numrows) {
        $DEBUG and print "FRA: No more cached rows, returning undef (row=$row, numrows=$numrows)\n";
        $STATS and $dbh->more('fetchrowarray_end');
        $sth->{private_curtuple} = 0;
        return undef;
    }

    ## Attempt to fetch this row from the database
    my $key = "FRA$row:$sth->{private_fetchpath}";
    $DEBUG >= 1 and warn "FRA key: $key\n";

    my $hit = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $hit ? 'fetchrowarray_hit' : 'fetchrowarray_miss');

    $sth->{private_curtuple} = $row + 1;

    if (defined $hit) {
        $DEBUG and print "FRA HIT: $hit\n";
        return $hit;
    }

    ## If numrows > 1, we'll have to pull the other ones. Ugly, but what can you do?
    if (!$sth->{private_lastexecutelive}) {
        my $oldrows = $sth->{private_numrows};
        my $rows = $sth->SUPER::execute(@{$sth->{private_executeargs}});
        if ($rows != $oldrows) {
            warn "DXC Warning: rows have changed from $oldrows to $rows\n";
            $sth->{private_numrows} = $rows;
        }
        $sth->{private_lastexecutelive} = 1;
        ## How many null fetches do we need to do?
        for (1..$row) {
            $sth->SUPER::fetchrow_array(@_);
        }
    }

    ## No cached info, so call the parent's fetch
    my $res = $sth->SUPER::fetchrow_array(@_);

    $dbh->store_key($key, $res, { tables => $sth } );

    return $res;

} ## end of fetchrow_arrayref


sub fetchrow_hashref {

    ## Same as fetchrow_arrayref, but we consider args as well

    my $sth = shift;
    my $dbh = $sth->{Database};

    ## If we are not caching, leave it to the parent
    if (! $sth->{private_cacheable}) {
        $STATS and $dbh->more('fetchrowhashref_nocache');
        return $sth->SUPER::fetchrow_hashref(@_);
    }

    $STATS and $dbh->more('fetchrowhashref_cache');

    ## If we know there are no more rows, just return an undef
    my $row = $sth->{private_curtuple};
    my $numrows = $sth->{private_numrows}; ## set by execute()
    if ($row >= $numrows) {
        $DEBUG and print "FRHR: No more cached rows, returning undef (row=$row, numrows=$numrows)\n";
        $STATS and $dbh->more('fetchrowhashref_end');
        $sth->{private_curtuple} = 0;
        return undef;
    }

    ## Attempt to fetch this row from the database
    my $key = "FRHR$row:$sth->{private_fetchpath}";
    for (@_) {
        $key .= '#' . Dumper($_);
    }
    $DEBUG >= 2 and warn "FETCHALL_ARRAYREF key: $key\n";

    my $hit = $dbh->fetch_key($key);

    $sth->{private_curtuple} = $row + 1;

    $STATS and $dbh->more(defined $hit ? 'fetchrowhashref_hit' : 'fetchrowhashref_miss');

    if (defined $hit) {
        $DEBUG and print "FRHR HIT: $hit\n";
        return $hit;
    }

    ## If numrows > 1, we'll have to pull the other ones. Ugly, but what can you do?
    if (!$sth->{private_lastexecutelive}) {
        my $oldrows = $sth->{private_numrows};
        my $rows = $sth->SUPER::execute(@{$sth->{private_executeargs}});
        if ($rows != $oldrows) {
            warn "DXC Warning: rows have changed from $oldrows to $rows\n";
            $sth->{private_numrows} = $rows;
        }
        $sth->{private_lastexecutelive} = 1;
        ## How many null fetches do we need to do?
        for (1..$row) {
            $sth->SUPER::fetchrow_hashref(@_);
        }
    }

    ## No cached info, so call the parent's fetch
    my $res = $sth->SUPER::fetchrow_hashref(@_);

    $dbh->store_key($key, $res, { tables => $sth } );

    return $res;

} ## end of fetchrow_hashref


sub fetchall_hashref {

    my $sth = shift;
    my $dbh = $sth->{Database};

    ## If we are not caching, leave it to the parent
    if (! $sth->{private_cacheable}) {
        $STATS and $dbh->more('fetchallhashref_nocache');
        return $sth->SUPER::fetchall_hashref(@_);
    }

    $STATS and $dbh->more('fetchallhashref_cache');

    ## Have we cached this value already? If so, return it
    my $key = "FAHR:$sth->{private_fetchpath}";

    ## Since the arguments affect the output, add those in as well
    for (@_) {
        $key .= '#' . Dumper($_);
    }
    $DEBUG >= 2 and print "FETCHALL_HASHREF key: $key\n";

    my $hit = $dbh->fetch_key($key);

    $STATS and $dbh->more(defined $hit ? 'fetchallhashref_hit' : 'fetchallhashref_miss');

    if (defined $hit) {
        $DEBUG and print "FETCHALL_HASHREF HIT: $hit\n";
        return $hit;
    }

    if (!$sth->{private_lastexecutelive}) {
        my $oldrows = $sth->{private_numrows};
        my $rows = $sth->SUPER::execute(@{$sth->{private_executeargs}});
        if ($rows != $oldrows) {
            warn "DXC Warning: rows have changed from $oldrows to $rows\n";
            $sth->{private_numrows} = $rows;
        }
        $sth->{private_lastexecutelive} = 1;
    }

    ## No cached info, so call the parent's fetch
    my $res = $sth->SUPER::fetchall_hashref(@_);

    $dbh->store_key($key, $res,    { tables => $sth });

    return $res;

} ## end of fetchall_hashref

sub finish {

    ## Finish up a statement handle
    ## Simply calls the parent

    my $sth = shift;

    $STATS and $sth->{Database}->more($sth->{private_cacheable} ? 'finish_cache' : 'finish_nocache');

    return $sth->SUPER::finish(@_);

} ## end of finish


sub _pg_parse_affected_tables {

    ## Given an parse tree, pull out which tables and functions will be used
    ## Returns an arrayref of items used
    ##
    ## First arg is database handle
    ## Second arg is statement handle
    ## Third arg is parse tree output
    ##
    ## This is also used to determine if the entire query is cacheable or not
    ## Returns 0 if the query should NOT be cached
    ##
    ## In the absense of hints, the following rules are used:
    ## It is NOT cacheable if any of the tables used are in system catalogs
    ## It is NOT cacheable if any volatile function is called
    ##
    ## Static keys used: nocache_table nocache_table_force oid2relnames volfuncs
    ##   nocache_function nocache_function_force
    ## Private attribs used: forcecache oid2relnames volfuncs
    ##
    ## TODO: User per-table inclusions and exclusions
    ## Both globally and per statement / handle / query

    my ($dbh,$sth,$tree) = @_;
    my $forcecache = $sth->{private_forcecache} || 0;

    ## Extract interesting information from the parse tree (rewritten)
    my (%rel, %func, @tables);
    for (split /\n/ => $tree) {
        if (/^\s*:relid (\d+)/o) {
            $rel{$1}++;
            next;
        }
        if (/^\s*:funcid (\d+)/o) {
            $func{$1}++;
            next;
        }
    }

    ## Check out all tables of interest, see if any are non-cacheable
    my ($new, $nocache) = (0,0);
    if (keys %rel) {
        my $rellist = $sth->{private_oid2relnames} || $dbh->fetch_key('oid2relnames') || {};
        for my $rel (keys %rel) {
            if (!exists $rellist->{$rel}) {
                my $SQL = 'SELECT nspname, relname, relkind FROM pg_class c, pg_namespace n '.
                    "WHERE c.oid=$rel AND c.relnamespace = n.oid --force_dbx_nocache";
                ## XXX Use parent instead of --force?
                $rellist->{$rel} = $dbh->selectall_arrayref($SQL)->[0];
                $new = 1;
            }
            my ($schema,$relname,$kind) = @{$rellist->{$rel}};
            push @tables => "$schema.$relname" if $kind eq 'r';

            ## Here is where we apply our table rules
            if ($kind eq 'r' and $schema =~ '^pg_') {
                $nocache = 1;
            }
        }
        if ($new) {
            $dbh->store_key('oid2relnames', $sth->{private_oid2relnames} = $rellist);
        }
        if ($nocache) {
            if ($forcecache) {
                $STATS and $dbh->more('nocache_table_force');
            }
            else {
                $STATS and $dbh->more('nocache_table');
                ## Do NOT return \@tables
                return 0;
            }
        }
    }

    ## Check all functions for cache-negating ability
    ($new,$nocache) = (0,0);
    if (keys %func) {
        my $funclist = $sth->{private_volfuncs} || $dbh->fetch_key('volfuncs') || {};
        for my $func (keys %func) {
            if (!exists $funclist->{$func}) {
                $dbh->do('SAVEPOINT dbix');
                my $SQL = "SELECT provolatile FROM pg_proc WHERE oid = $func --nodbixcache";
                $funclist->{$func} = $dbh->selectall_arrayref($SQL)->[0][0];
                $dbh->do('ROLLBACK TO dbix');
                $new = 1;
            }

            ## Here is where we apply our function rules
            if ($funclist->{$func} eq 'v') {
                $nocache = 1;
            }
        }
        if ($new) {
            $dbh->store_key('volfuncs', $sth->{private_volfuncs} = $funclist);
        }
        if ($nocache) {
            if ($forcecache) {
                $STATS and $dbh->more('nocache_function_force');
            }
            else {
                $STATS and $dbh->more('nocache_function');
                return 0;
            }
        }
    }

    return \@tables;

} ## end of _pg_parse_affected_tables



#######################################
package DBIx::Cache::DB_File;

use Data::Dumper;

my $VARNAME = 'DBIXC';

sub new {

    my $class = shift;
    my $file  = shift;
    my $self  = {};

    require DB_File;

    my %h;
    tie (%h, 'DB_File', $file) or die "Failed to tie DB_File: $!";
    $self->{bukkit} = \%h;

    if ($DEBUG) {
        my $msg = sprintf 'Created DB_File which is based on %s',
            defined $file ? "file '$file'" : 'in-memory database';
        warn "$msg\n";
    }

    return bless $self, $class;
}

sub namespace {
    ## Set up the global namespace
    ## Not really used here - we use the global $NAMESPACE instead for now
    my ($self,$name) = @_;
    $NAMESPACE = $self->{namespace} = $name;
    return;
}

sub get {
    my ($self,$key) = @_;
    return undef if ! exists $self->{bukkit}{"$NAMESPACE$key"};
    if (0 == index($key,'@')) {
        return eval $self->{bukkit}{"$NAMESPACE$key"};
    }
    else {
        return $self->{bukkit}{"$NAMESPACE$key"};
    }

}

sub get_multi {

    my $self = shift;
    my $hashref;
    for my $key (@_) {
        if (! exists $self->{bukkit}{"$NAMESPACE$key"}) {
            $hashref->{$key} = undef;
            next;
        }
        if (0 == index($key,'@')) {
            $hashref->{key} = eval $self->{bukkit}{"$NAMESPACE$key"};
        }
        else {
            $hashref->{$key} = $self->{bukkit}{"$NAMESPACE$key"};
        }
    }
    return $hashref;
}


sub add {

    ## Change a value, only if it does not already exist
    my ($self,$key,$val) = @_;
    return if exists $self->{bukkit}{"$NAMESPACE$key"};
    return $self->set($key,$val);
}

sub set {

    ## Change a value, creating if need be

    my ($self,$key,$val) = @_;
    if (0 == index($key,'@')) {
        local $Data::Dumper::Indent=0;
        local $Data::Dumper::Purity=1;
        local $Data::Dumper::Varname=$VARNAME;
        $self->{bukkit}{"$NAMESPACE$key"} = Dumper $val;
    }
    else {
        $self->{bukkit}{"$NAMESPACE$key"} = $val;
    }
    return;
}

sub set_multi {

    ## Change one or more values, creating if need be

    my $self = shift;

    my ($key,$val) = ('','');
    {
        my $key = shift;
        my $val = shift;
        last if ! defined $val;
        $self->set($key,$val);
        redo;
    }
    return;
}

sub delete {
    my ($self,$key) = @_;
    delete $self->{bukkit}{"$NAMESPACE$key"};
    return;
}

sub delete_multi {
    my $self = shift;
    for my $key (@_) {
        delete $self->{bukkit}{"$NAMESPACE$key"};
    }
    return;
}

sub replace {
    my ($self,$key,$val) = @_;
    return if ! exists $self->{bukkit}{"$NAMESPACE$key"};
    if (0 == index($key,'@')) {
        local $Data::Dumper::Indent=0;
        local $Data::Dumper::Purity=1;
        local $Data::Dumper::Varname=$VARNAME;
        $self->{bukkit}{"$NAMESPACE$key"} = Dumper $val;
    }
    else {
        $self->{bukkit}{"$NAMESPACE$key"} = $val;
    }
    return;
}

sub replace_multi {
    my $self= shift;
    for (@_) {
        my ($key,$val,$exp) = @$_;
        ## Of course, there is no way to expire these...
        next if ! exists $self->{bukkit}{"$NAMESPACE$key"};
        if (0 == index($key,'@')) {
            local $Data::Dumper::Indent=0;
            local $Data::Dumper::Purity=1;
            local $Data::Dumper::Varname=$VARNAME;
            $self->{bukkit}{"$NAMESPACE$key"} = Dumper $val;
        }
        else {
            $self->{bukkit}{"$NAMESPACE$key"} = $val;
        }
    }
    return;
}
sub dump_bukkit {
    my $self = shift;
    return $self->{bukkit};
}

sub incr {
    my ($self,$key) = @_;
    warn "INKY: $key ($STATS)\n";
    $self->{bukkit}{"$NAMESPACE$key"}++;
    return;
}

sub decr {
    my ($self,$key) = @_;
    $self->{bukkit}{"$NAMESPACE$key"}--;
    return;
}



1;

__END__

=head1 DBIx::Cache

=head2 Main methods

=head3 B<connect>

Connect to the database, exactly the same as DBI->connect. Note that the RaiseError attribute 
is always forced on. Some attributes specific to DBIx::Cache may be passed in:

=over 4

=item C<dxc_cachetype>

Indicates which type if cache should be used. Current choices are C<DB_File> and C<memcached>.

=item C<dxc_cachehandle>

A caching object, e.g. a Cache::Memcached::Fast object.

=item C<dxc_filename>

If using cachetype of C<DB_File>, this is the location of the file. If not set, DBIx::Cache 
used an in-memory BDB file.

=item C<dxc_stats>

Turns the internal statistics gathering on or off. The default is off.

=item C<dxc_reset_stats>

Resets all the internal statistics keys to 0, by calling the L</reset_dxc_stats> method.

=item C<dxc_no_test>

The connect method performs a simple set and get test as part of its setup. For maximum 
speed, you can bypass this test by setting this attribute.

=back

=head2 Database handle methods

=head3 B<reset_dbc_stats>

Resets all the internal statistics to zero, and creates the variables in the caching server 
if they do not already exist.

=head3 B<dxc_stats>

  $dbh->dxc_stats($boolean)

Turns the internal statistics gathering on or off.

=head3 B<get_dxc_stats>

  $hashref = $dbh->get_dxc_stats();

Returns a hashref containing all internal statistics. The statistic names are the keys, and the 
value is an arrayref containing the current value and a description of the statistic.

=head3 B<fetch_key>

  $value = $dbh->fetch_key($key);

Given the name of a key, fetch it's value from the cache. If they key does not exist, 
undef will be returned.

=head3 B<store_key>

  $rv = $dbh->store_key($ky, $value);
  $rv = $dbh->store_key($ky, $value, \%attr);

Store a key with the given value in the cache. Returns an undef on error. The optional 
C<\%attr> argument may contain an C<expire> argument, which indicates in how many 
seconds the entry it set to expire.

=head3 B<more>

  $dbh->more($key);

Calls C<incr> on the key, increasing its value by one.

=head3 B<less>

  $dbh->less($key);

Calls C<decr> on the key, decreasing its value by one.

=head3 B<delete_key>

  $rv = $dbh->delete_key($key);

Removes a key from the cache. Returns undef on error. Performs cascading deletion magic 
as needed if the key is linked to others, as for a table connected to specific query 
caches.

=head3 B<prepare>

  $sth = $dbh->prepare($sql);

Called exactly the same as the DBI version. Statements can contain hints to control the caching:

=over 4

=item * --force_dxc_cache

If this string appears in the SQL statement, caching is forced to be done whenever possible.

=item * --force_dxc_nocache

If this string appears in the SQL statement, caching is not done.

=back

=head2 Statement handle methods

=head3 B<_pg_parse_affected_tables>

Internal function.

=head3 B<execute>

  $numrows = $sth->execute();

Called exactly the same as the DBI version. This is where most of the initial caching magic happens.

=head3 B<finish>

Same as DBI.

=head3 B<fetch>

Same as DBI.

=head3 B<fetchall_arrayref>

Same as DBI.

=head3 B<fetchrow_arrayref>

Same as DBI.

=head3 B<fetchall_hashref>

Same as DBI.

=head3 B<fetchrow_hashref>

Same as DBI.

=head3 B<fetchrow_array>

=head2 Requirements

DBIx::Cache has the following requirements:

=over 4

=item Caching engine:

- memcached + Cache::Memcached::Fast
- DB_File

=item Time::HiRes

=item DBI and DBD::Pg

=item Digest::SHA

=item The 'version' module

=item Test::More

=back

