package DBIx::ThinSQL::Deploy;
use strict;
use warnings;
use Log::Any qw/$log/;
use Carp qw/croak carp confess/;
use File::ShareDir qw/dist_dir/;
use Path::Tiny;

our $VERSION = '0.0.5_1';

sub last_deploy_id {
    my $self = shift;
    my $app = shift || 'default';

    my $sth = $self->table_info( '%', '%', '_deploy' );
    return 0 unless ( @{ $sth->fetchall_arrayref } );

    return $self->selectrow_array(
        'SELECT COALESCE(MAX(seq),0) FROM _deploy WHERE app=?',
        undef, $app );
}

sub _load_file {
    my $file = path(shift);
    my $type = lc $file;

    $log->debug( '_load_file(' . $file . ')' );
    confess "fatal: missing extension/type: $file\n"
      unless $type =~ s/.*\.(.+)$/$1/;

    my $input = $file->slurp_utf8;
    my $end   = '';
    my $item  = '';
    my @items;

    if ( $type eq 'sql' ) {

        $input =~ s/^\s*--.*\n//gm;
        $input =~ s!/\*.*?\*/!!gsm;

        while ( $input =~ s/(.*\n)// ) {
            my $try = $1;

            if ($end) {
                if ( $try =~ m/$end/ ) {
                    $item .= $try;

                    if ( $try =~ m/;/ ) {
                        $item =~ s/(^[\s\n]+)|(\s\n]+$)//;
                        push( @items, { sql => $item } );
                        $item = '';
                    }

                    $end = '';
                }
                else {
                    $item .= $try;
                }

            }
            elsif ( $try =~ m/;/ ) {
                $item .= $try;
                $item =~ s/(^[\s\n]+)|(\s\n]+$)//;
                push( @items, { sql => $item } );
                $item = '';
            }
            elsif ( $try =~ m/^\s*CREATE( OR REPLACE)? FUNCTION.*AS (\S*)/i ) {
                $end = $2;
                $end =~ s/\$/\\\$/g;
                $item .= $try;
            }
            elsif ( $try =~ m/^\s*CREATE TRIGGER/i ) {
                $end = qr/(EXECUTE PROCEDURE)|(^END)/i;
                $item .= $try;
            }
            else {
                $item .= $try;
            }
        }
    }
    elsif ( $type eq 'pl' ) {
        push( @items, { $type => $input } );
    }
    else {
        die "Cannot load file of type '$type': $file";
    }

    $log->debug( scalar @items . ' statements' );
    return @items;
}

sub _run_cmds {
    my $self = shift;
    my $ref  = shift;

    local $self->{ShowErrorStatement} = 1;
    local $self->{RaiseError}         = 1;

    $log->debug( 'running ' . scalar @$ref . ' statements' );
    my $i = 1;

    foreach my $cmd (@$ref) {
        if ( exists $cmd->{sql} ) {
            $log->debug( "-- _run_cmd $i\n" . $cmd->{sql} );
            $self->do( $cmd->{sql} );
        }
        elsif ( exists $cmd->{pl} ) {
            $log->debug( "-- _run_cmd\n" . $cmd->{pl} );
            my $tmp = Path::Tiny->tempfile;
            print $tmp $cmd->{pl};
            system( $^X, $tmp->filename ) == 0 or die "system failed";
        }
        else {
            confess "Missing 'sql' or 'pl' key";
        }

        $i++;
    }

    return scalar @$ref;
}

sub run_file {
    my $self = shift;
    my $file = shift;

    $log->debug("run_file($file)");
    $self->_run_cmds( _load_file($file) );
}

sub run_dir {
    my $self = shift;
    my $dir = path(shift) || confess 'deploy_dir($dir)';

    confess "directory not found: $dir" unless -d $dir;
    $log->debug("run_dir($dir)");

    my @files;
    my $iter = $dir->iterator;
    while ( my $file = $iter->() ) {
        push( @files, $file )
          if $file =~ m/.+\.((sql)|(pl))$/ and -f $file;
    }

    my @items =
      map  { _load_file($_) }
      sort { $a->stringify cmp $b->stringify } @files;

    $self->_run_cmds( \@items );
}

sub _setup_deploy {
    my $self   = shift;
    my $driver = $self->{Driver}->{Name};

    $log->debug("_setup_deploy");
    if ( my $share = $Test::DBIx::ThinSQL::SHARE_DIR ) {
        $self->run_dir( $share->child( 'Deploy', $driver ) );
    }
    else {
        $self->run_dir( path( dist_dir('DBIx-ThinSQL'), 'Deploy', $driver ) );
    }

    return;
}

sub deploy {
    my $self = shift;
    my $ref  = shift;
    my $app  = shift || 'default';

    $log->debug("deploy($app)");
    $self->_setup_deploy;
    $self->_deploy( $ref, $app );
}

sub _deploy {
    my $self = shift;
    my $ref  = shift;
    my $app  = shift || 'default';

    confess 'deploy(ARRAYREF)' unless ref $ref eq 'ARRAY';
    local $self->{ShowErrorStatement} = 1;
    local $self->{RaiseError}         = 1;

    my @current =
      $self->selectrow_array( 'SELECT COUNT(app) from _deploy WHERE app=?',
        undef, $app );

    unless ( $current[0] ) {
        $self->do( '
                    INSERT INTO _deploy(app)
                    VALUES(?)
                ', undef, $app );
    }

    my $latest_change_id = $self->last_deploy_id($app);
    $log->debug( 'Current Change ID:',   $latest_change_id );
    $log->debug( 'Requested Change ID:', scalar @$ref );

    die "Requested Change ID("
      . ( scalar @$ref )
      . ") is less than current: $latest_change_id"
      if @$ref < $latest_change_id;

    my $count = 0;
    foreach my $cmd (@$ref) {
        $count++;
        next unless ( $count > $latest_change_id );

        exists $cmd->{sql}
          || exists $cmd->{pl}
          || confess "Missing 'sql' or 'pl' key for id " . $count;

        if ( exists $cmd->{sql} ) {
            $log->debug( "-- change #$count\n" . $cmd->{sql} );
            $self->do( $cmd->{sql} );
            $self->do( "
UPDATE 
    _deploy
SET
    type = ?,
    data = ?
WHERE
    app = ?
",
                undef, 'sql', $cmd->{sql}, $app );
        }

        if ( exists $cmd->{pl} ) {
            $log->debug( "# change #$count\n" . $cmd->{pl} );
            my $tmp = Path::Tiny->tempfile;
            $tmp->spew_utf8( $cmd->{pl} );

            # TODO stop and restart the transaction (if any) around
            # this
            system( $^X, $tmp ) == 0 or die "system failed";
            $self->do( "
UPDATE 
    _deploy
SET
    type = ?,
    data = ?
WHERE
    app = ?
",
                undef, 'pl', $cmd->{pl}, $app );
        }
    }
    $log->debug( 'Deployed to Change ID:', $count );
    return ( $latest_change_id, $count );
}

sub deploy_file {
    my $self = shift;
    my $file = shift;
    my $app  = shift;
    $log->debug("deploy_file($file)");
    $self->_setup_deploy;
    $self->_deploy( [ _load_file($file) ], $app );
}

sub deploy_dir {
    my $self = shift;
    my $dir  = path(shift) || confess 'deploy_dir($dir)';
    my $app  = shift;

    confess "directory not found: $dir" unless -d $dir;
    $log->debug("deploy_dir($dir)");
    $self->_setup_deploy;

    my @files;
    my $iter = $dir->iterator;
    while ( my $file = $iter->() ) {
        push( @files, $file )
          if $file =~ m/.+\.((sql)|(pl))$/ and -f $file;
    }

    my @items =
      map  { _load_file($_) }
      sort { $a->stringify cmp $b->stringify } @files;

    $self->_deploy( \@items, $app );
}

sub deployed_table_info {
    my $self     = shift;
    my $dbschema = shift;
    my $driver   = $self->{Driver}->{Name};

    if ( !$dbschema ) {
        if ( $driver eq 'SQLite' ) {
            $dbschema = 'main';
        }
        elsif ( $driver eq 'Pg' ) {
            $dbschema = 'public';
        }
        else {
            $dbschema = '%';
        }
    }

    my $sth = $self->table_info( '%', $dbschema, '%',
        "'TABLE','VIEW','GLOBAL TEMPORARY','LOCAL TEMPORARY'" );

    my %tables;

    while ( my $table = $sth->fetchrow_arrayref ) {
        my $sth2 = $self->column_info( '%', '%', $table->[2], '%' );
        $tables{ $table->[2] } = $sth2->fetchall_arrayref;
    }

    return \%tables;
}

{
    no strict 'refs';
    *{'DBIx::ThinSQL::db::last_deploy_id'}      = \&last_deploy_id;
    *{'DBIx::ThinSQL::db::_load_file'}          = \&_load_file;
    *{'DBIx::ThinSQL::db::_run_cmds'}           = \&_run_cmds;
    *{'DBIx::ThinSQL::db::run_file'}            = \&run_file;
    *{'DBIx::ThinSQL::db::run_dir'}             = \&run_dir;
    *{'DBIx::ThinSQL::db::_setup_deploy'}       = \&_setup_deploy;
    *{'DBIx::ThinSQL::db::deploy'}              = \&deploy;
    *{'DBIx::ThinSQL::db::_deploy'}             = \&_deploy;
    *{'DBIx::ThinSQL::db::deploy_file'}         = \&deploy_file;
    *{'DBIx::ThinSQL::db::deploy_dir'}          = \&deploy_dir;
    *{'DBIx::ThinSQL::db::deployed_table_info'} = \&deployed_table_info;
}

1;
