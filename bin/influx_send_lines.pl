#!/usr/bin/env perl
use strict;
use warnings;
use lib::projectroot qw(lib local::lib=local);

# PODNAME: influx_send_lines.pl
# ABSTRACT: Manually send files to influxdb for non-live stats or replay
# VERSION

package Runner;
use Moose;
extends 'InfluxDB::Writer::SendLines';
with 'MooseX::Getopt';

use Log::Any::Adapter ('Stderr');

my $runner = Runner->new_with_options->run;

