#! /usr/bin/env perl

use Test::More;

use FindBin;
use File::Spec::Functions qw(catfile catdir);
use File::Slurper qw/write_text/;
use IO::Async::Loop;

use InfluxDB::Writer::FileTailer;

use Test::MockModule;
use Test::TempDir::Tiny;

my $datadir = catdir( $FindBin::Bin, 'testdata', 'something' );

my %basic_args = (
    influx_host => 'computer1',
    influx_port => '27015',
    influx_db   => 'test'
);

subtest 'pid_dead' => sub {
    my $tmpdir = tempdir('pid_dead');

    my $stats_file = catfile( $tmpdir, 'pid_dead_123.stats' );
    write_text( $stats_file, "test pid dead" );

    my $mock = Test::MockModule->new('InfluxDB::Writer::FileTailer');
    $mock->mock( 'is_running', sub {0} );

    my $ft = InfluxDB::Writer::FileTailer->new( dir => $tmpdir, %basic_args );
    my $filestream = $ft->setup_file_watcher($stats_file);
    ok( !$filestream, 'no pid should not return a filestream' );
};

subtest 'pid_alive_already_content_in_file' => sub {
    my $tmpdir = tempdir('pid_dead');

    my $text            = "test pid dead\n";
    my $stats_file      = catfile( $tmpdir, 'pid_dead_123.stats' );
    my @expected_result = ();
    open my $fd, ">", $stats_file
        or die "Cannot open file $stats_file for writing";
    $fd->autoflush(1);
    $fd->syswrite("1 $text");
    push @expected_result, "1 $text";

    my $mock = Test::MockModule->new('InfluxDB::Writer::FileTailer');
    $mock->mock( 'is_running', sub {1} );
    $mock->mock( 'send',       sub { } );

    my $ft = InfluxDB::Writer::FileTailer->new( dir => $tmpdir, %basic_args );
    my $filestream = $ft->setup_file_watcher($stats_file);
    ok( $filestream, 'got a filestream for stats file' );
    is( @{ $ft->buffer }, 0, 'no new lines, nothing should be read' );

    my $loop = IO::Async::Loop->new_builtin();
    $loop->add($filestream);

    $fd->syswrite("2 $text");
    push @expected_result, "2 $text";

    $loop->loop_once();

    is( @{ $ft->buffer }, 2, 'buffer should contain two lines' );
    is_deeply( $ft->buffer, \@expected_result, 'compare buffer content' );
};

done_testing();
