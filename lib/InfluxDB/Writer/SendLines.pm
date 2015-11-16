package InfluxDB::Writer::SendLines;
use strict;
use warnings;
use feature 'say';

our $VERSION = '1.000';

# ABSTRACT: Send lines from a file to InfluxDB

use Moose;
use Carp qw(croak);
use Log::Any qw($log);
use File::Spec::Functions;
use Hijk ();
use MIME::Base64 qw/encode_base64/;

has 'file'        => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_host' => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_port' =>
    ( is => 'ro', isa => 'Int', default => 8086, required => 1 );
has 'influx_db'       => ( is => 'ro', isa => 'Str', required => 1 );
has 'influx_username' => ( is => 'ro', isa => 'Str', required => 0 );
has 'influx_password' => ( is => 'ro', isa => 'Str', required => 0 );
has 'buffer_size'     => ( is => 'ro', isa => 'Int', default  => 1000 );

has '_with_auth' => ( is => 'rw', isa => 'Bool' );
has '_auth_header' => ( is => 'ro', isa => 'Str', lazy_build => 1 );

sub _build__auth_header {
    my $self = shift;
    if ( $self->influx_username && $self->influx_password ) {
        my $base64 = encode_base64(
            join( ":", $self->influx_username, $self->influx_password ) );
        chomp($base64);
        $self->_auth_header("Basic $base64");
        $self->_with_auth(1);
    }
}

$| = 1;

my @buffer;

sub run {
    my $self = shift;

    $log->infof( "Starting %s with file %s", __PACKAGE__, $self->file );

    my $f     = $self->file;
    my $lines = `wc -l $f`;
    chomp($lines);
    $lines =~ s/ .*//;
    my $total = $lines;
    my $start = scalar time;

    open( my $in, "<", $self->file ) || die $!;

    my $cnt       = 0;
    my $print_cnt = $self->buffer_size * 50;
    while ( my $line = <$in> ) {
        push( @buffer, $line );
        if ( @buffer == $self->buffer_size ) {
            $self->send;
        }
        $cnt++;
        if ( $cnt % $print_cnt == 0 ) {
            my $now   = scalar time;
            my $diff  = $now - $start || 1;
            my $speed = $cnt / $diff;
            my $estimate =
                $speed > 0 ? ( $total - $cnt ) / $speed : 'Infinity';
            printf( "  % 6i/%i (%.2f/s) time left: %i sec\n",
                $cnt, $total, $speed, $estimate );
        }
    }
    $self->send;
}

sub send {
    my $self       = shift;
    my $second_try = shift;
    my $new_buffer = shift;

    my $to_send = $second_try ? $new_buffer : \@buffer;

    my %args;
    if ( $self->_with_auth ) {
        $args{head} = [ "Authorization" => $self->_auth_header ];
    }

    $log->debugf( "Sending %i lines to influx", scalar @$to_send );
    my $res = Hijk::request(
        {   method       => "POST",
            host         => $self->influx_host,
            port         => $self->influx_port,
            path         => "/write",
            query_string => "db=" . $self->influx_db,
            body         => join( '', @$to_send ),
            %args,
        }
    );
use Data::Dumper; $Data::Dumper::Maxdepth=3;$Data::Dumper::Sortkeys=1;warn Data::Dumper::Dumper $res;

    if ( $res->{status} != 204 ) {
        if (!$second_try
            && (
                (   exists $res->{error}
                    && $res->{error} & Hijk::Error::TIMEOUT
                )
                || ( $res->{status} == 500 && $res->{body} =~ /timeout/ )
            )
            ) {
            # wait a bit and try again with smaller packages
            my @half = splice( @buffer, 0, int( scalar @buffer / 2 ) );
            print ':';
            $self->send( 1, \@half );
            $self->send( 1, \@buffer );
        }
        else {
            $log->errorf(
                "Could not send %i lines to influx: %s",
                scalar @buffer,
                $res->{body}
            );
            open( my $fh, ">>", $self->file . '.err' ) || die $!;
            print $fh join( '', @buffer );
            close $fh;
            print 'X';
        }
    }
    else {
        print $second_try ? ',' : '.';
    }
    @buffer = () unless $second_try;
}

__PACKAGE__->meta->make_immutable;
1;
