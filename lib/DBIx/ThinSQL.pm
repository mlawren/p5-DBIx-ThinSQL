package DBIx::ThinSQL;
use strict;
use warnings;
use DBI;
use Exporter::Tidy
  default => [qw/ bv qv qi /],
  other   => [qw/ OR AND /],
  sql     => [
    qw/
      func
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
    cast => sub { DBIx::ThinSQL::_expr->func( 'cast', ' ', @_ ) },
    case => sub {
        shift @_;
        unshift @_, 'case when';

        my @sql;
        my @bv;

        while ( my ( $key, $val ) = splice( @_, 0, 2 ) ) {
            my $expr =
              DBIx::ThinSQL::_expr->ejoin( "\n        ", uc($key), $val );

            push( @sql, $expr->sql, "\n    " );
            push( @bv, $expr->bv );
        }
        push( @sql, 'END' );

        return DBIx::ThinSQL::_expr->new( \@sql, \@bv );
    },
    coalesce => sub { DBIx::ThinSQL::_expr->func( 'coalesce', ', ', @_ ) },
    count    => sub { DBIx::ThinSQL::_expr->func( 'count',    ', ', @_ ) },
    exists   => sub { DBIx::ThinSQL::_expr->func( 'exists',   ', ', @_ ) },
    hex      => sub { DBIx::ThinSQL::_expr->func( 'hex',      ', ', @_ ) },
    length   => sub { DBIx::ThinSQL::_expr->func( 'length',   ', ', @_ ) },
    lower    => sub { DBIx::ThinSQL::_expr->func( 'lower',    ', ', @_ ) },
    ltrim    => sub { DBIx::ThinSQL::_expr->func( 'ltrim',    ', ', @_ ) },
    max      => sub { DBIx::ThinSQL::_expr->func( 'max',      ', ', @_ ) },
    min      => sub { DBIx::ThinSQL::_expr->func( 'min',      ', ', @_ ) },
    replace  => sub { DBIx::ThinSQL::_expr->func( 'replace',  ', ', @_ ) },
    rtrim    => sub { DBIx::ThinSQL::_expr->func( 'rtrim',    ', ', @_ ) },
    substr   => sub { DBIx::ThinSQL::_expr->func( 'substr',   ', ', @_ ) },
    sum      => sub { DBIx::ThinSQL::_expr->func( 'sum',      ', ', @_ ) },
    upper    => sub { DBIx::ThinSQL::_expr->func( 'upper',    ', ', @_ ) },
  };

our @ISA     = 'DBI';
our $VERSION = '0.0.1';

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
    my @sql;
    my @bv;

    eval {
        while ( my ( $key, $val ) = splice( @_, 0, 2 ) )
        {
            ( my $tmp = uc($key) ) =~ s/_/ /g;
            my $VALUES = $tmp eq 'VALUES';
            if ( !$VALUES ) {
                push( @sql, $tmp );
                push( @sql, "\n" );
            }

            next unless defined $val;

            if ( ref $val eq 'DBIx::ThinSQL::_bv' ) {

                push( @bv, $val );

                # add to @bv
                push( @sql, '?' );
            }
            elsif ( ref $val eq 'ARRAY' ) {
                if ($VALUES) {
                    my $expr =
                      DBIx::ThinSQL::_expr->ejoin( ', ',
                        map { DBIx::ThinSQL::_bv->new($_) } @$val );

                    push( @sql, "VALUES\n    (", $expr->sql, ')' );
                    push( @bv, $expr->bv );
                }
                elsif ( $key =~ m/^select/i ) {
                    my $expr = DBIx::ThinSQL::_expr->ejoin( ",\n    ", @$val );
                    push( @sql, '    ', $expr->sql );
                    push( @bv, $expr->bv );
                }
                elsif ( $key =~ m/^order_by/i ) {
                    my $expr = DBIx::ThinSQL::_expr->ejoin( ",\n    ", @$val );
                    push( @sql, '    ', $expr->sql );
                    push( @bv, $expr->bv );
                }
                else {
                    my $expr = DBIx::ThinSQL::_expr->ejoin( undef, @$val );
                    push( @sql, '    ', $expr->sql );
                    push( @bv, $expr->bv );
                }
            }
            elsif ( ref $val eq 'HASH' ) {
                if ($VALUES) {
                    my ( @columns, @values );
                    while ( my ( $k, $v ) = each %$val ) {
                        push( @columns, $k );
                        push( @values,  $v );
                    }

                    my $expr =
                      DBIx::ThinSQL::_expr->ejoin( ', ',
                        map { DBIx::ThinSQL::_bv->new($_) } @values );

                    push( @sql, '    (', join( ', ', @columns ), ")\n" );
                    push( @sql, "VALUES\n    (", $expr->sql, ')' );
                    push( @bv, $expr->bv );
                }

         #                elsif ( $key =~ m/^select/i ) {
         #                    push( @sql, '    ',
         #                    DBIx::ThinSQL::_expr->ejoin( ",\n    ", @$val ) );
         #                }
         #                elsif ( $key =~ m/^order_by/i ) {
         #                    push( @sql, '    ',
         #                    DBIx::ThinSQL::_expr->ejoin( ",\n    ", @$val ) );
         #                }
         #                else {
         #                    my ( $s, $b ) = _get_bv(@$val);
         #                    push( @sql, '    ',
         #                    DBIx::ThinSQL::_expr->ejoin( ' ', @$s ) );
         #                    push( @bv, @$b );
         #                }
            }
            else {
                push( @sql, '    ' . $val );
            }

            push( @sql, "\n" );
        }
    };

    Carp::croak "Bad Query: $@" if $@;
    return \@sql, \@bv;
}

sub xprepare {
    my $self = shift;

    my ( $sqlref, $bindref ) = _query(@_);
    my $qv_count = 0;
    my $qi_count = 0;

    my $sql = join(
        '',
        map {
            if ( ref $_ eq 'DBIx::ThinSQL::_qv' )
            {
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
        } @$sqlref
    );

    my $bv_count = scalar @$bindref;

    $log->debug( "/* xprepare() with bv: $bv_count qv: $qv_count "
          . "qi: $qi_count */\n"
          . $sql );

    my $sth = eval {
        local $self->{RaiseError}         = 1;
        local $self->{PrintError}         = 0;
        local $self->{ShowErrorStatement} = 1;
        my $sth = $self->prepare($sql);

        my $i = 1;
        foreach my $bv (@$bindref) {
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

# another kind of constructor
sub ejoin {
    my $class = shift;
    my $token = shift;
    return bless( [ [], [] ], $class ) unless @_;

    my $last = $#_;
    my @sql;
    my @bv;

    my $i = 0;
    foreach my $item (@_) {
        if ( ref $item eq 'ARRAY' ) {    # CASE WHEN ... in a SELECT?
            push( @sql, $class->ejoin( undef, @$item ) );
        }
        elsif ( ref $item eq 'DBIx::ThinSQL::_bv' ) {
            push( @sql, '?' );
            push( @bv,  $item );
        }
        elsif ( ref $item eq 'DBIx::ThinSQL::_expr' ) {
            push( @sql, $item->sql );
            push( @bv,  $item->bv );
        }
        else {
            push( @sql, $item );
        }
        push( @sql, $token ) unless !defined $token or $i == $last;
        $i++;
    }

    return bless [ \@sql, \@bv ], $class;
}

# and yet another kind of constructor
sub func {
    my $class = shift;
    my $func  = uc shift;
    my $token = shift;

    my $expr = $class->ejoin( $token, @_ );
    return bless [ [ $func, '(', $expr->sql, ')' ], [ $expr->bv ] ], $class;
}

sub sql {
    return @{ $_[0]->[0] };
}

sub bv {
    return @{ $_[0]->[1] };
}

sub as {
    my $self  = shift;
    my $value = shift;

    push( @{ $self->[0] }, ' AS ', DBIx::ThinSQL::_qi->new($value) );
    return $self;
}

1;
