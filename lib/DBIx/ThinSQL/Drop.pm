package DBIx::ThinSQL::Drop;
use strict;
use warnings;
use File::ShareDir qw/dist_dir/;
use Path::Tiny;
use DBIx::ThinSQL::Deploy;

our $VERSION = '0.0.10';

sub drop_everything {
    my $self      = shift;
    my $driver    = $self->{Driver}->{Name};
    my $share_dir = $Test::DBIx::ThinSQL::SHARE_DIR
      || dist_dir('DBIx-ThinSQL');

    my $dir = path( $share_dir, 'Drop', $driver );
    if ( !-d $dir ) {
        require Carp;
        Carp::croak "Drop for driver $driver is unsupported.";
    }
    return $self->run_dir($dir);
}

{
    no strict 'refs';
    *{'DBIx::ThinSQL::db::drop_everything'} = \&drop_everything;
}

1;
