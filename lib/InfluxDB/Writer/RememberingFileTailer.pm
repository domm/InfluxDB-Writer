package InfluxDB::Writer::RememberingFileTailer;

use Moose;
our $VERSION = '1.000';
use feature 'say';

use IO::Async::File;
use IO::Async::FileStream;
use IO::Async::Loop;
use Hijk ();
use Carp qw(croak);
use InfluxDB::LineProtocol qw(line2data data2line);
use Log::Any qw($log);
use File::Spec::Functions;
use File::Spec qw(splitpath);

extends 'InfluxDB::Writer::FileTailer';
my @buffer = @{\@InfluxDB::Writer::FileTailer::buffer};

has 'done_dir' => ( is => 'rw', isa => "Str" );

before 'run' => sub {
    my $self = shift;

    my $done_dir = catdir($self->dir, 'done');
    if ( -d $done_dir ) {
        $self->done_dir($done_dir);
    } else {
        croak "Missing 'done' directory, please create: " . $done_dir;
    }

};

sub archive_file {
    my $self = shift;
    my $file = shift;

    my $done_dir = $self->done_dir;
    my ($vol, $dirs, $basename) = File::Spec->splitpath($file);

    if ( rename($file, catfile($done_dir, $basename)) ) {
        $log->infof("Archived file %s to %s", $basename ,$done_dir);
        return 1;
    }
    else {
        $log->errorf("Failed to archive %s to %s", $basename ,$done_dir);
        croak "Failed to archive $file";
        return 0;
    }

}

=head2 slurp_and_send

IO::Async read operations are blocking so it does not make sense to wrice
complicated async code here. We want to read the files that are left over and
send them to influx asap, then move them to the archive folder (aka. out of the
way)

=cut

sub slurp_and_send {
    my $self = shift;
    my $file = shift;

    if ( open( my $fh, "<", $file ) ) {
        $log->infof( "Slurping %s", $file );

        while (my $line = <$fh>) {
            if ( $self->has_tags ) {
                $line = $self->add_tags_to_line($line);
            }
            push(@buffer, $line);

            if ( @buffer > $self->flush_size ) {
                if (!$self->send) {
                    $log->warnf("Unable to send buffer (%i lines)", scalar @buffer);
                    return;
                }
            }
        }

        if (scalar @buffer ) {
            $log->infof( "Clear buffer (size %i) for file %s", scalar(@buffer), $file );
            if (!$self->send) {
                $log->warnf("Unable to send clear buffer (%i lines)", scalar @buffer);
                return;
            }
        }

        $log->infof( "Finished slurping %s", $file );

        return 1;
    }

    return;

}


override 'archive_hook' => sub {
    shift->archive_file(@_);
    super();
};

override 'cleanup_hook' => sub {
    my ($self, $file) = @_;
    
    if ($self->slurp_and_send($file)) {
        $self->archive_file($file);
    }
    return super();

};


1;
