package DBIx::ThinSQL;
use strict;
use warnings;
use DBI;
use Exporter::Tidy
  default => [qw/ bv qv qi /],
  other   => [qw/ func OR AND /],
  sql     => [
    qw/
      case
      cast
      coalesce
      concat
      count
      exists
      hex
      length
      lower
      ltrim
      max
      min
      replace
      rtrim
      substr
      sum
      upper

      /
  ],
  _map => {
    bv   => sub { DBIx::ThinSQL::_bv->new(@_) },
    qv   => sub { DBIx::ThinSQL::_qv->new(@_) },
    qi   => sub { DBIx::ThinSQL::_qi->new(@_) },
    OR   => sub { ' OR ' },
    AND  => sub { ' AND ' },
    cast => sub { func( 'cast', ' ', @_ ) },
    case => sub {
        my @tokens;

        shift @_;
        unshift @_, 'case when';

        while ( my ( $key, $val ) = splice( @_, 0, 2 ) ) {
            push( @tokens, _ejoin( "\n        ", uc($key), $val ), "\n    " );
        }
        push( @tokens, 'END' );

        return DBIx::ThinSQL::_expr->new(@tokens);
    },
    coalesce => sub { func( 'coalesce', ', ', @_ ) },
    concat => sub { DBIx::ThinSQL::_expr->new( _ejoin( ' || ', @_ ) ) },
    count   => sub { func( 'count',   ', ', @_ ) },
    exists  => sub { func( 'exists',  ', ', @_ ) },
    hex     => sub { func( 'hex',     ', ', @_ ) },
    length  => sub { func( 'length',  ', ', @_ ) },
    lower   => sub { func( 'lower',   ', ', @_ ) },
    ltrim   => sub { func( 'ltrim',   ', ', @_ ) },
    max     => sub { func( 'max',     ', ', @_ ) },
    min     => sub { func( 'min',     ', ', @_ ) },
    replace => sub { func( 'replace', ', ', @_ ) },
    rtrim   => sub { func( 'rtrim',   ', ', @_ ) },
    substr  => sub { func( 'substr',  ', ', @_ ) },
    sum     => sub { func( 'sum',     '',   @_ ) },
    upper   => sub { func( 'upper',   ', ', @_ ) },
  };

our @ISA     = 'DBI';
our $VERSION = '0.0.5_1';

sub _ejoin {
    my $joiner = shift;
    return unless @_;

    my @tokens;
    my $last = $#_;

    my $i = 0;
    foreach my $item (@_) {
        if ( ref $item eq 'ARRAY' ) {    # CASE WHEN ... in a SELECT?
            push( @tokens, _ejoin( undef, @$item ) );
        }
        elsif ( ref $item eq 'DBIx::ThinSQL::_expr' ) {
            push( @tokens, $item->tokens );
        }
        else {
            push( @tokens, $item );
        }

        push( @tokens, $joiner ) unless !defined $joiner or $i == $last;
        $i++;
    }

    return @tokens;
}

sub func {
    my $func   = uc shift;
    my $joiner = shift;

    return DBIx::ThinSQL::_expr->new( $func, '(', _ejoin( $joiner, @_ ), ')' );
}

package DBIx::ThinSQL::db;
use strict;
use warnings;
use Carp ();
use Log::Any qw/$log/;

our @ISA = qw(DBI::db);
our @CARP_NOT;

sub _query {

    # use Data::Dumper;
    # warn Dumper \@_;
    my @tokens;

    eval {
        while ( my ( $key, $val ) = splice( @_, 0, 2 ) )
        {

            ( my $tmp = uc($key) ) =~ s/_/ /g;
            my $VALUES = $tmp eq 'VALUES';
            if ( !$VALUES ) {
                push( @tokens, $tmp );
                push( @tokens, "\n" );
            }

            next unless defined $val;

            if ( ref $val eq 'ARRAY' ) {
                if ($VALUES) {
                    push(
                        @tokens,
                        "VALUES\n    (",
                        DBIx::ThinSQL::_ejoin(
                            ', ', map { DBIx::ThinSQL::_bv->new($_) } @$val
                        ),
                        ')'
                    );
                }
                elsif ( $key =~ m/^((select)|(order_by)|(group_by))/i ) {
                    push( @tokens,
                        '    ', DBIx::ThinSQL::_ejoin( ",\n    ", @$val ) );
                }
                else {
                    push( @tokens,
                        '    ', DBIx::ThinSQL::_ejoin( undef, @$val ) );
                }
            }
            elsif ( ref $val eq 'HASH' ) {
                if ($VALUES) {
                    my ( @columns, @values );
                    while ( my ( $k, $v ) = each %$val ) {
                        push( @columns, $k );                            # qi()?
                        push( @values,  DBIx::ThinSQL::_bv->new($v) );
                    }

                    push( @tokens,
                        '    (',
                        join( ', ', @columns ),
                        ")\nVALUES\n    (",
                        DBIx::ThinSQL::_ejoin( ', ', @values ), ')' );
                }
                else {
                    my ( $i, @columns, @values );
                    while ( my ( $k, $v ) = each %$val ) {
                        push( @columns, $k );                            # qi()?
                        push( @values,  DBIx::ThinSQL::_bv->new($v) );
                        $i++;
                    }
                    push( @tokens, '    ' );
                    while ( $i-- ) {
                        push( @tokens,
                            shift @columns,
                            ' = ', shift @values,
                            ' AND ' );
                    }
                    pop @tokens;
                }
            }
            else {
                push( @tokens, '    ' . $val );
            }

            push( @tokens, "\n" );
        }
    };

    Carp::croak "Bad Query: $@" if $@;
    return @tokens;
}

sub xprepare {
    my $self     = shift;
    my $bv_count = 0;
    my $qv_count = 0;
    my $qi_count = 0;

    my @bv;

    my $sql = join(
        '',
        map {
            if ( ref $_ eq 'DBIx::ThinSQL::_bv' )
            {
                $bv_count++;
                push( @bv, $_ );
                '?';
            }
            elsif ( ref $_ eq 'DBIx::ThinSQL::_qv' ) {
                $qv_count++;
                $self->quote( $_->for_quote );
            }
            elsif ( ref $_ eq 'DBIx::ThinSQL::_qi' ) {
                $qi_count++;
                $self->quote_identifier( $_->val );
            }
            else {
                $_;
            }
        } _query(@_)
    );

    $log->debug( "/* xprepare() with bv: $bv_count qv: $qv_count "
          . "qi: $qi_count */\n"
          . $sql );

    my $sth = eval {

        # TODO these locals have no effect?
        local $self->{RaiseError}         = 1;
        local $self->{PrintError}         = 0;
        local $self->{ShowErrorStatement} = 1;
        my $sth = $self->prepare($sql);

        my $i = 1;
        foreach my $bv (@bv) {
            $sth->bind_param( $i++, $bv->for_bind_param );
        }

        $sth;
    };

    Carp::croak($@) if $@;

    return $sth;
}

sub xdo {
    my $self = shift;

    return $self->xprepare(@_)->execute;
}

sub xarray {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my @ref = $sth->array;
    $sth->finish;

    return unless @ref;
    return @ref if wantarray;
    return \@ref;
}

sub xarrays {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;

    return $sth->arrays;
}

sub xhash {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my $ref = $sth->hash;
    $sth->finish;

    return $ref;
}

sub xhashes {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    return $sth->hashes;
}

# Can't use 'local' to managed txn count here because $self is a tied hash?
# Also can't use ||=.
sub txn {
    my $self      = shift;
    my $subref    = shift;
    my $wantarray = wantarray;
    my $txn       = $self->{private_DBIx_ThinSQL_txn}++;
    my $driver    = $self->{private_DBIx_ThinSQL_driver};

    $driver ||= $self->{private_DBIx_ThinSQL_driver} = do {
        my $class = 'DBIx::ThinSQL::Driver::' . $self->{Driver}->{Name};
        eval { $class->new } || DBIx::ThinSQL::Driver->new;
    };

    my $current;
    if ( !$txn ) {
        $current = {
            RaiseError         => $self->{RaiseError},
            ShowErrorStatement => $self->{ShowErrorStatement},
        };

    }

    $self->{RaiseError} = 1 unless exists $self->{HandleError};
    $self->{ShowErrorStatement} = 1;

    my @result;
    my $result;

    if ( !$txn ) {
        $log->debug('BEGIN');
        $self->begin_work;
    }
    else {
        $log->debug( 'SAVEPOINT ' . $txn );
        $driver->savepoint( $self, 'txn' . $txn );
    }

    eval {

        if ($wantarray) {
            @result = $subref->();
        }
        else {
            $result = $subref->();
        }

        if ( !$txn ) {
            $log->debug('COMMIT');
            $self->commit;
        }
        else {
            $log->debug( 'RELEASE ' . $txn );
            $driver->release( $self, 'txn' . $txn );
        }

    };
    my $error = $@;

    $self->{private_DBIx_ThinSQL_txn} = $txn;
    if ( !$txn ) {
        $self->{RaiseError}         = $current->{RaiseError};
        $self->{ShowErrorStatement} = $current->{ShowErrorStatement};
    }

    if ($error) {

        eval {
            if ( !$txn )
            {
                # If the transaction failed at COMMIT, then we can no
                # longer roll back. Maybe put this around the eval for
                # the RELEASE case as well??
                if ( !$self->{AutoCommit} ) {
                    $log->debug('ROLLBACK');
                    $self->rollback unless $self->{AutoCommit};
                }
            }
            else {
                $log->debug( 'ROLLBACK TO ' . $txn );
                $driver->rollback_to( $self, 'txn' . $txn );
            }
        };

        Carp::croak(
            $error . "\nAdditionally, an error occured during
                  rollback:\n$@"
        ) if $@;

        Carp::croak($error);
    }

    return $wantarray ? @result : $result;
}

package DBIx::ThinSQL::st;
use strict;
use warnings;

our @ISA = qw(DBI::st);

sub array {
    my $self = shift;
    return unless $self->{Active};

    my $ref = $self->fetchrow_arrayref || return;

    return @$ref if wantarray;
    return $ref;
}

sub arrays {
    my $self = shift;
    return unless $self->{Active};

    my $all = $self->fetchall_arrayref || return;

    return @$all if wantarray;
    return $all;
}

sub hash {
    my $self = shift;
    return unless $self->{Active};

    return $self->fetchrow_hashref('NAME_lc');
}

sub hashes {
    my $self = shift;
    return unless $self->{Active};

    my @all;
    while ( my $ref = $self->fetchrow_hashref('NAME_lc') ) {
        push( @all, $ref );
    }

    return @all if wantarray;
    return \@all;
}

package DBIx::ThinSQL::_bv;
use strict;
use warnings;

#use overload '""' => sub {Carp::croak('_bv '. $_[0]->[0].'stringed')};

sub new {
    my $class = shift;
    return $_[0] if ( ref $_[0] ) =~ m/^DBIx::ThinSQL::_/;
    return bless [@_], $class;
}

sub val {
    return $_[0]->[0];
}

sub type {
    return $_[0]->[1];
}

sub for_bind_param {
    my $self = shift;

    # value, type
    return @$self if defined $self->[1];

    # value
    return $self->[0];
}

package DBIx::ThinSQL::_qv;
use strict;
use warnings;

sub new {
    my $class = shift;
    return $_[0] if ( ref $_[0] ) =~ m/^DBIx::ThinSQL::_/;
    return bless [@_], $class;
}

sub val {
    return $_[0]->[0];
}

sub type {
    return $_[0]->[1];
}

sub for_quote {
    my $self = shift;

    # value, type
    return @$self if defined $self->[1];

    # value
    return $self->[0];
}

package DBIx::ThinSQL::_qi;
use strict;
use warnings;

sub new {
    my $class = shift;
    return $_[0] if ( ref $_[0] ) =~ m/^DBIx::ThinSQL::_/;

    my $id = shift;
    return bless \$id, $class;
}

sub val {
    my $self = shift;
    return $$self;
}

package DBIx::ThinSQL::_expr;
use strict;
use warnings;

sub new {
    my $class = shift;
    return bless [@_], $class;
}

# and yet another kind of constructor
sub as {
    my $self  = shift;
    my $value = shift;

    push( @$self, ' AS ', DBIx::ThinSQL::_qi->new($value) );
    return $self;
}

sub tokens {
    my $self = shift;
    return @$self;
}

package DBIx::ThinSQL::Driver;
use strict;
use warnings;

sub new {
    my $class = shift;
    return bless {}, $class;
}

sub savepoint {
}

sub release {
}

sub rollback_to {
}

package DBIx::ThinSQL::Driver::SQLite;
use strict;
use warnings;

our @ISA = ('DBIx::ThinSQL::Driver');

sub savepoint {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->do( 'SAVEPOINT ' . $name );
}

sub release {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->do( 'RELEASE ' . $name );
}

sub rollback_to {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->do( 'ROLLBACK TO ' . $name );
}

package DBIx::ThinSQL::Driver::Pg;
use strict;
use warnings;

our @ISA = ('DBIx::ThinSQL::Driver');

sub savepoint {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->pg_savepoint($name);
}

sub release {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->pg_release($name);
}

sub rollback_to {
    my $self = shift;
    my $dbh  = shift;
    my $name = shift;
    $dbh->pg_rollback_to($name);
}

1;
