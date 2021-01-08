#!/usr/bin/env perl
use strict;
use warnings;
use lib::projectroot qw(lib local::lib=local);

# PODNAME: influx_file_tailer.pl
# ABSTRACT: Tail files and send them to influxdb for live stats
# VERSION


package Runner;
use Moose;
extends 'InfluxDB::Writer::FileTailer';
with 'MooseX::Getopt';

use Log::Any::Adapter ('Stderr');

my $runner = Runner->new_with_options->run;

