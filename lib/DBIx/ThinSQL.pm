package DBIx::ThinSQL;
use strict;
use warnings;
use DBI;
use Carp ();
use Exporter::Tidy
  default => [qw/ bv qv OR AND /],
  sql     => [
    qw/
      sql_func
      /
  ];
use Log::Any qw/$log/;

our @ISA     = 'DBI';
our $VERSION = '0.0.1';

sub _ljoin {
    my $token = shift;
    return ( [], [] ) unless @_;

    my $last = $#_;
    my @sql;
    my @bv;

    my $i = 0;
    foreach my $item (@_) {
        if ( ref $item eq 'ARRAY' ) {    # CASE WHEN ... in a SELECT?
            push( @sql, _ljoin( undef, @$item ) );
        }
        elsif ( ref $item eq 'DBIx::ThinSQL::_bv' ) {
            push( @sql, '?' );
            push( @bv,  $item );
        }
        elsif ( ref $item eq 'DBIx::ThinSQL::_func' ) {
            push( @sql, $item->sql );
            push( @bv,  $item->bv );
        }
        else {
            push( @sql, $item );
        }
        push( @sql, $token ) unless !defined $token or $i == $last;
        $i++;
    }

    return \@sql, \@bv;
}

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
                    my ( $sql, $bv ) =
                      _ljoin( ', ', map { DBIx::ThinSQL::_bv->new($_) } @$val );

                    push( @sql, "VALUES\n    (", @$sql, ')' );
                    push( @bv, @$bv );
                }
                elsif ( $key =~ m/^select/i ) {
                    my ( $sql, $bv ) = _ljoin( ",\n    ", @$val );
                    push( @sql, '    ', @$sql );
                    push( @bv, @$bv );
                }
                elsif ( $key =~ m/^order_by/i ) {
                    my ( $sql, $bv ) = _ljoin( ",\n    ", @$val );
                    push( @sql, '    ', @$sql );
                    push( @bv, @$bv );
                }
                else {
                    my ( $sql, $bv ) = _ljoin( undef, @$val );
                    push( @sql, '    ', @$sql );
                    push( @bv, @$bv );
                }
            }
            elsif ( ref $val eq 'HASH' ) {
                if ($VALUES) {
                    my ( @columns, @values );
                    while ( my ( $k, $v ) = each %$val ) {
                        push( @columns, $k );
                        push( @values,  $v );
                    }

                    my ( $sql, $bv ) =
                      _ljoin( ', ',
                        map { DBIx::ThinSQL::_bv->new($_) } @values );

                    push( @sql, '    (', join( ', ', @columns ), ")\n" );
                    push( @sql, "VALUES\n    (", @$sql, ')' );
                    push( @bv, @$bv );
                }

          #                elsif ( $key =~ m/^select/i ) {
          #                    push( @sql, '    ', _ljoin( ",\n    ", @$val ) );
          #                }
          #                elsif ( $key =~ m/^order_by/i ) {
          #                    push( @sql, '    ', _ljoin( ",\n    ", @$val ) );
          #                }
          #                else {
          #                    my ( $s, $b ) = _get_bv(@$val);
          #                    push( @sql, '    ', _ljoin( ' ', @$s ) );
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

sub sql_case {
    _query( 'case_' . shift, map { '    ' . $_ } @_, 'END' );
}

sub sql_func {
    my $func = uc shift;
    return DBIx::ThinSQL::_func->new( $func, @_ );
}

sub bv  { DBIx::ThinSQL::_bv->new(@_); }
sub qv  { DBIx::ThinSQL::_qv->new(@_); }
sub OR  { ' OR ' }
sub AND { ' AND ' }

package DBIx::ThinSQL::db;
use strict;
use warnings;

our @ISA = qw(DBI::db);
our @CARP_NOT;

sub xprepare {
    my $self = shift;

    my ( $sqlref, $bindref ) = DBIx::ThinSQL::_query(@_);
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

    $DBIx::ThinSQL::log->debug(
            "/* xprepare() with bind: $bv_count quote: $qv_count "
          . "ident: $qi_count */\n"
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

package DBIx::ThinSQL::_func;
use strict;
use warnings;

sub new {
    my $class = shift;
    my $func  = shift;
    my @list  = ( $func, '(' );

    my ( $sql, $bv ) = DBIx::ThinSQL::_ljoin( ', ', @_ );
    unshift( @$sql, $func, '(' );
    push( @$sql, ')' );

    return bless [ $sql, $bv ], $class;
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
