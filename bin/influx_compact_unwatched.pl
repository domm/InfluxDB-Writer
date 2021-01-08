#!/usr/bin/env perl
use strict;
use warnings;
use lib::projectroot qw(lib local::lib=local);

# PODNAME: influx_compact_unwatched.pl
# ABSTRACT: Compact all files that are currently not watched
# VERSION

package Runner;
use Moose;
extends 'InfluxDB::Writer::CompactFiles';
with 'MooseX::Getopt';

use Log::Any::Adapter ('Stderr');

my $runner = Runner->new_with_options->run;

