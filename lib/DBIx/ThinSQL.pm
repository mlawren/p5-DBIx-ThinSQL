package DBIx::ThinSQL;
use strict;
use warnings;
use DBI;
use Exporter::Tidy
  other => [qw/ bv qv qi sq func OR AND /],
  sql   => [
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
    bv => sub {
        DBIx::ThinSQL::_expr->new( DBIx::ThinSQL::_bv->new(@_) );
    },
    qv => sub {
        DBIx::ThinSQL::_expr->new( DBIx::ThinSQL::_qv->new(@_) );
    },
    qi => sub {
        DBIx::ThinSQL::_expr->new( DBIx::ThinSQL::_qi->new(@_) );
    },
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
our $VERSION = '0.0.33_1';

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
        elsif ( ref $item eq 'HASH' ) {
            my ( $i, @columns, @values );
            foreach my $k ( sort keys %$item ) {
                my $v = $item->{$k};
                push( @columns, $k );    # qi()?
                if ( ref $v eq 'SCALAR' ) {
                    push( @values, $$v );
                }
                elsif ( ref $v eq 'ARRAY' ) {
                    push( @values,
                        [ map { DBIx::ThinSQL::_bv->new($_) } @$v ] );
                }
                else {
                    push( @values, DBIx::ThinSQL::_bv->new($v) );
                }
                $i++;
            }
            push( @tokens, ' ' );    #$prefix2 );
            while ( $i-- ) {
                my $like = $columns[0] =~ s/\s+like$/ LIKE /i ? 1 : 0;

                my $not_like =
                  $columns[0] =~ s/\s+(!|not)\s*like$/ NOT LIKE /i ? 1 : 0;

                my $not = $columns[0] =~ s/\s+!$// ? 1 : 0;

                push( @tokens, shift @columns );
                if ( ref $values[0] eq 'ARRAY' ) {

                    push( @tokens,
                        $not ? ' NOT' : '',
                        ' IN (', _ejoin( ',', @{ shift @values } ),
                        ')', ' AND ' );
                }
                elsif ( !ref $values[0] || defined $values[0]->val ) {
                    push( @tokens, $not ? ' != ' : ' = ' )
                      unless $like or $not_like;

                    push( @tokens, shift @values, ' AND ' );
                }
                else {
                    push( @tokens, ' IS ', $not ? 'NOT NULL' : 'NULL',
                        ' AND ' );
                }
            }
            pop @tokens;
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

our $prefix1 = '';
our $prefix2 = ' ' x 4;

sub _query {

    # use Data::Dumper;
    # warn Dumper \@_;
    my @tokens;

    eval {
        while ( my ( $key, $val ) = splice( @_, 0, 2 ) ) {

            ( my $tmp = uc($key) ) =~ s/_/ /g;
            my $VALUES = $tmp eq 'VALUES';
            my $SET    = $tmp eq 'SET';
            if ( !$VALUES ) {
                push( @tokens, $prefix1 . $tmp . "\n" );
            }

            next unless defined $val;

            if ( ref $val eq 'DBIx::ThinSQL::_expr' ) {
                push( @tokens, $val->tokens );
            }
            elsif ( ref $val eq 'ARRAY' ) {
                if ($VALUES) {
                    if (@$val) {
                        push(
                            @tokens,
                            "VALUES\n$prefix2(",
                            DBIx::ThinSQL::_ejoin(
                                ', ', map { DBIx::ThinSQL::_bv->new($_) } @$val
                            ),
                            ')'
                        );
                    }
                    else {
                        push( @tokens, "DEFAULT VALUES\n", );
                    }
                }
                elsif ( $key =~ m/((select)|(order_by)|(group_by))/i ) {
                    push( @tokens,
                        $prefix2,
                        DBIx::ThinSQL::_ejoin( ",\n$prefix2", @$val ) );
                }
                elsif ( $key =~ m/insert/i ) {
                    push( @tokens,
                        $prefix2,
                        ( shift @$val ) . "(\n$prefix2    ",
                        DBIx::ThinSQL::_ejoin( ",\n$prefix2    ", @$val ),
                        "\n$prefix2)" );
                }
                else {
                    push( @tokens,
                        $prefix2, DBIx::ThinSQL::_ejoin( undef, @$val ) );
                }
            }
            elsif ( ref $val eq 'HASH' ) {
                if ($VALUES) {
                    if ( keys %$val ) {
                        my ( @columns, @values );
                        foreach my $k ( sort keys %$val ) {
                            my $v = $val->{$k};
                            push( @columns, $k );    # qi()?
                            if ( ref $v eq 'SCALAR' ) {
                                push( @values, $$v );
                            }
                            elsif ( ref $v eq 'ARRAY' ) {
                                push( @values,
                                    [ map { DBIx::ThinSQL::_bv->new($_) } @$v ]
                                );
                            }
                            else {
                                push( @values, DBIx::ThinSQL::_bv->new($v) );
                            }
                        }

                        push( @tokens,
                            $prefix2 . '(',
                            join( ', ', @columns ),
                            ")\nVALUES\n$prefix2(",
                            DBIx::ThinSQL::_ejoin( ', ', @values ),
                            ')' );
                    }
                    else {
                        push( @tokens, "DEFAULT VALUES\n", );
                    }
                }
                elsif ($SET) {
                    my ( $i, @columns, @values );
                    foreach my $k ( sort keys %$val ) {
                        my $v = $val->{$k};
                        push( @columns, $k );    # qi()?
                        if ( ref $v eq 'SCALAR' ) {
                            push( @values, $$v );
                        }
                        elsif ( ref $v eq 'ARRAY' ) {
                            push( @values,
                                [ map { DBIx::ThinSQL::_bv->new($_) } @$v ] );
                        }
                        else {
                            push( @values, DBIx::ThinSQL::_bv->new($v) );
                        }
                        $i++;
                    }
                    push( @tokens, $prefix2 );
                    while ( $i-- ) {
                        push( @tokens, shift @columns );
                        push( @tokens, ' = ', shift @values, ', ' );
                    }
                    pop @tokens;
                }
                else {
                    my ( $i, @columns, @values );
                    foreach my $k ( sort keys %$val ) {
                        my $v = $val->{$k};
                        push( @columns, $k );    # qi()?
                        if ( ref $v eq 'SCALAR' ) {
                            push( @values, $$v );
                        }
                        elsif ( ref $v eq 'ARRAY' ) {
                            push( @values,
                                [ map { DBIx::ThinSQL::_bv->new($_) } @$v ] );
                        }
                        else {
                            push( @values, DBIx::ThinSQL::_bv->new($v) );
                        }
                        $i++;
                    }
                    push( @tokens, $prefix2 );
                    while ( $i-- ) {
                        my $like = $columns[0] =~ s/\s+like$/ LIKE /i ? 1 : 0;

                        my $not_like =
                          $columns[0] =~ s/\s+(!|not)\s*like$/ NOT LIKE /i
                          ? 1
                          : 0;

                        my $not = $columns[0] =~ s/\s+!$// ? 1 : 0;
                        push( @tokens, shift @columns );
                        if ( ref $values[0] eq 'ARRAY' ) {
                            push( @tokens,
                                $not ? ' NOT' : '',
                                ' IN (', _ejoin( ',', @{ shift @values } ),
                                ')', ' AND ' );
                        }
                        elsif ( !ref $values[0] || defined $values[0]->val ) {
                            push( @tokens, $not ? ' != ' : ' = ' )
                              unless $like or $not_like;

                            push( @tokens, shift @values, ' AND ' );
                        }
                        else {
                            push( @tokens,
                                ' IS ', $not ? 'NOT NULL' : 'NULL',
                                ' AND ' );
                        }
                    }
                    pop @tokens;
                }
            }
            else {
                if ($VALUES) {
                    push( @tokens, $prefix1 . $tmp . "\n" );
                }

                push( @tokens, $prefix2 . $val );
            }

            push( @tokens, "\n" );
        }
    };

    Carp::croak "Bad Query: $@" if $@;
    return @tokens;
}

sub sq {
    my $oldprefix1 = $prefix1;
    local $prefix1 = $prefix1 . ( ' ' x 4 );
    local $prefix2 = $prefix2 . ( ' ' x 4 );
    my $first = '(' . shift;
    return DBIx::ThinSQL::_expr->new(
        $oldprefix1,
        _query( $first, @_ ),
        $prefix1 . ')'
    );
}

package DBIx::ThinSQL::db;
use strict;
use warnings;
use Carp ();
use Log::Any '$log';

our @ISA = qw(DBI::db);
our @CARP_NOT;

sub _sql_bv {
    my $self     = shift;
    my $bv_count = 0;
    my $qv_count = 0;
    my $qi_count = 0;

    my @bv;

    my $sql = join(
        '',
        map {
            if ( ref $_ eq 'DBIx::ThinSQL::_bv' ) {
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
        } DBIx::ThinSQL::_query(@_)
    );

    return $sql, @bv;
}

sub xprepare {
    my $self = shift;
    my ( $sql, @bv ) = $self->_sql_bv(@_);

    # TODO these locals have no effect?
    local $self->{RaiseError}         = 1;
    local $self->{PrintError}         = 0;
    local $self->{ShowErrorStatement} = 1;

    my $sth = eval {
        my $sth = $self->prepare($sql);
        my $i   = 1;
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
    my ( $sql, @bv ) = $self->_sql_bv(@_);

    # TODO these locals have no effect?
    local $self->{RaiseError}         = 1;
    local $self->{PrintError}         = 0;
    local $self->{ShowErrorStatement} = 1;

    return $self->do($sql) unless @bv;

    my $sth = eval {
        my $sth = $self->prepare($sql);
        my $i   = 1;
        foreach my $bv (@bv) {
            $sth->bind_param( $i++, $bv->for_bind_param );
        }

        $sth;
    };

    Carp::croak($@) if $@;

    return $sth->execute;
}

sub log_debug {
    my $self = shift;
    my $sql  = (shift) . "\n";

    my $sth = $self->prepare( $sql . ';' );
    $sth->execute(@_);

    my $out = join( ', ', @{ $sth->{NAME} } ) . "\n";
    $out .= '  ' . ( '-' x length $out ) . "\n";
    $out .= '  ' . DBI::neat_list($_) . "\n" for @{ $sth->fetchall_arrayref };
    $log->debug($out);
}

sub dump {
    my $self = shift;
    my $sth  = $self->prepare(shift);
    $sth->execute(@_);
    $sth->dump_results;
}

sub xdump {
    my $self = shift;
    my $sth  = $self->xprepare(@_);
    $sth->execute;
    $sth->dump_results;
}

sub xval {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my $ref = $sth->arrayref;
    $sth->finish;

    return $ref->[0] if $ref;
    return;
}

sub xvals {
    my $self = shift;
    my $sth  = $self->xprepare(@_);
    $sth->execute;
    return $sth->vals;
}

sub xlist {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my $ref = $sth->arrayref;
    $sth->finish;

    return @$ref if $ref;
    return;
}

sub xarrayref {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my $ref = $sth->arrayref;
    $sth->finish;

    return $ref if $ref;
    return;
}

sub xarrayrefs {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;

    return $sth->arrayrefs;
}

sub xhashref {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    my $ref = $sth->hashref;
    $sth->finish;

    return $ref if $ref;
    return;
}

sub xhashrefs {
    my $self = shift;

    my $sth = $self->xprepare(@_);
    $sth->execute;
    return $sth->hashrefs;
}

# Can't use 'local' to managed txn count here because $self is a tied hashref?
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
        $self->begin_work;
    }
    else {
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

            # We check again for the AutoCommit state in case the
            # $subref did something like its own ->rollback(). This
            # really just prevents a warning from being printed.
            $self->commit unless $self->{AutoCommit};
        }
        else {
            $driver->release( $self, 'txn' . $txn ) unless $self->{AutoCommit};
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
            if ( !$txn ) {

                # If the transaction failed at COMMIT, then we can no
                # longer roll back. Maybe put this around the eval for
                # the RELEASE case as well??
                $self->rollback unless $self->{AutoCommit};
            }
            else {
                $driver->rollback_to( $self, 'txn' . $txn )
                  unless $self->{AutoCommit};
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

sub val {
    my $self = shift;
    my $ref = $self->fetchrow_arrayref || return;
    return $ref->[0];
}

sub vals {
    my $self = shift;
    my $all = $self->fetchall_arrayref || return;
    return unless @$all;
    return map { $_->[0] } @$all if wantarray;
    return [ map { $_->[0] } @$all ];
}

sub list {
    my $self = shift;
    my $ref = $self->fetchrow_arrayref || return;
    return @$ref;
}

sub arrayref {
    my $self = shift;
    return unless $self->{Active};
    return $self->fetchrow_arrayref;
}

sub arrayrefs {
    my $self = shift;
    return unless $self->{Active};

    my $all = $self->fetchall_arrayref || return;
    return unless @$all;
    return @$all if wantarray;
    return $all;
}

sub hashref {
    my $self = shift;
    return unless $self->{Active};

    return $self->fetchrow_hashref('NAME_lc');
}

sub hashrefs {
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
