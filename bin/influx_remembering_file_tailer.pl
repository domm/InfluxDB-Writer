#!/usr/bin/env perl
use strict;
use warnings;
use lib::projectroot qw(lib local::lib=local);

# ABSTRACT: Tail files and send them to influxdb for live stats

package Runner;
use Moose;
extends 'InfluxDB::Writer::RememberingFileTailer';
with 'MooseX::Getopt';

use Log::Any::Adapter ('Stderr');

my $runner = Runner->new_with_options->run;

