package DBIx::ThinSQL::Drop;
package    # hide this monkey patching
  DBIx::ThinSQL::db;
use strict;
use warnings;
use File::ShareDir qw/dist_dir/;
use Path::Tiny;
use DBIx::ThinSQL::Deploy;

our $VERSION = '0.0.1';

sub drop_everything {
    my $self   = shift;
    my $driver = $self->{Driver}->{Name};

    if ( my $share = $Test::DBIx::ThinSQL::SHARE_DIR ) {
        $self->run_dir( $share->child( 'Drop', $driver ) );
    }
    else {
        $self->run_dir( dist_dir('SQL-DB'), 'Drop', $driver );
    }
    return;
}

1;
