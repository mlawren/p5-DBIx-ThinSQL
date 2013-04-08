package DBIx::ThinSQL;
use strict;
use warnings;
use DBI;
use Carp ();
use Exporter::Tidy all => [qw/ bv qv OR AND /];
use Log::Any qw/$log/;

our @ISA     = 'DBI';
our $VERSION = '0.0.1';

sub _get_bv {
    my @sql;
    my @bv;
    my $space = '';

    foreach my $token (@_) {
        if ( ref $token eq 'DBIx::ThinSQL::_bv' ) {
            push( @sql, '?' );
            push( @bv,  $token );
        }
        else {
            push( @sql, $token );
        }
    }
    return \@sql, \@bv;
}

sub _make_bv {
    my $ref = shift;

    foreach my $i ( 0 .. $#{$ref} ) {
        $ref->[$i] = DBIx::ThinSQL::_bv->new( $ref->[$i] )
          unless ( ref $ref->[$i] ) =~ m/^DBIx::ThinSQL::_(b|q)v$/;
    }
    return;
}

sub _ljoin {
    my $token = shift;
    return unless @_;

    my $last = pop @_;
    return $last unless @_;

    return ( map { $_ => $token } @_ ), $last;
}

sub _query {

    #    use Data::Dumper;
    #    warn Dumper \@_;
    my @sql;
    my @bind;

    eval {
        while ( my ( $key, $val ) = splice( @_, 0, 2 ) )
        {
            ( my $tmp = uc($key) ) =~ s/_/ /g;
            push( @sql, $tmp );
            push( @sql, "\n" ) unless lc $key eq 'values';

            next unless defined $val;

            my $brackets;
            $brackets++ if lc $key eq 'values';

            push( @sql, '(' ) if $brackets;

            if ( ref $val eq 'DBIx::ThinSQL::_bv' ) {

                push( @bind, $val );

                # add to @bind
                push( @sql, '?' );
            }
            elsif ( ref $val eq 'ARRAY' ) {
                if ( $key =~ m/^values$/i ) {
                    _make_bv($val);
                    push( @sql, _ljoin( ', ', map { '?' } 0 .. $#{$val} ) );
                    push( @bind, @$val );
                }
                elsif ( $key =~ m/^select/i ) {
                    push( @sql, '    ', _ljoin( ",\n    ", @$val ) );
                }
                elsif ( $key =~ m/^order_by/i ) {
                    push( @sql, '    ', _ljoin( ",\n    ", @$val ) );
                }
                else {
                    my ( $s, $b ) = _get_bv(@$val);
                    push( @sql, '    ', _ljoin( ' ', @$s ) );
                    push( @bind, @$b );
                }
            }
            else {
                push( @sql, '    ' . $val );
            }

            push( @sql, ')' ) if $brackets;
            push( @sql, "\n" );
        }
    };

    Carp::croak "Bad Query: $@" if $@;
    return \@sql, \@bind;
}

sub sql_case {
    _query( 'case_' . shift, map { '    ' . $_ } @_, 'END' );
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

# For testing - see t/01
sub _private_dbix_sqlx_sponge {
    my $self = shift;
    $self->{private_dbix_sqlx_sponge} = shift;
}

sub xprepare {
    my $self = shift;

    my ( $sqlref, $bindref ) = DBIx::ThinSQL::_query(@_);
    my $qv_count = 0;

    my $sql = join(
        '',
        map {
            if ( ref $_ eq 'DBIx::ThinSQL::_qv' )
            {
                $qv_count++;
                $self->quote( $_->for_quote );
            }
            else {
                $_;
            }
        } @$sqlref
    );

    my $bv_count = scalar @$bindref;

    $DBIx::ThinSQL::log->debug(
        "/* xprepare() with bind: $bv_count quote: $qv_count */\n" . $sql );

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

sub insert {
    my $self       = shift;
    my $str_into   = shift;
    my $table      = shift;
    my $str_values = shift;
    my $values     = shift;

    unless ($str_into eq 'into'
        and $str_values eq 'values'
        and ( eval { $values->isa('HASH') } ) )
    {
        Carp::croak 'usage: insert(into => $table, values => $hashref)';
    }

    my $urow = $self->urow($table);

    my @cols    = sort grep { $urow->can($_) } keys %$values;
    my @invalid = sort grep { !$urow->can($_) } keys %$values;
    my @vals = map { _bval( $values->{$_}, $urow->$_->_type ) } @cols;

    $DBIx::ThinSQL::log->debug(
        "columns not in table '$table': @invalid\n    at", caller )
      if @invalid;
    Carp::croak 'insert_into requires columns/values' unless @cols;

    return 0 + $self->do(
        insert_into => sql_table( $table, @cols ),
        sql_values(@vals),
    );
}

# $db->update('purchases',
#     set   => {pid => 2},
#     where => {cid => 1},
# );
sub update {
    my $self  = shift;
    my $table = shift;
    shift;
    my $set = shift;
    shift;
    my $where = shift;

    my $urow = $self->urow($table);
    my @updates = map { $urow->$_( $set->{$_} ) }
      grep { $urow->can($_) and !exists $where->{$_} } keys %$set;

    unless (@updates) {
        $DBIx::ThinSQL::log->debug( "Nothing to update for table:", $table );
        return 0;
    }

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr =
          _expr_join( ' AND ',
            map { $urow->$_ == $where->{$_} } grep { $urow->can($_) } @keys );
    }

    return 0 + $self->do(
        update => $urow,
        set    => \@updates,
        $expr ? ( where => $expr ) : (),
    );
}

# $db->delete(
#    from => 'purchases',
#    where => {cid => 1},
# );

sub delete {
    my $self = shift;
    shift;    # from
    my $table = shift;
    shift;    # where
    my $where = shift;

    my @expr;
    my @keys = keys %$where;

    while (@keys) {
        push( @expr, 'where' ) unless @expr;

        my $key = shift @keys;

        push( @expr,
            $self->quote_identifier($key),
            ' = ', qv( $where->{$key} ) );

        push( @expr, ' AND ' ) if @keys;
    }

    return $self->do(
        delete_from => $table,
        @expr,
    );
}

# my @objs = $db->select( ['pid','label],
#     from => 'customers',
#     where => {cid => 1},
# );
sub select {
    my $self = shift;
    my $list = shift;
    shift;
    my $table = shift;
    shift;
    my $where = shift;

    my $srow = $self->srow($table);
    my @columns = map { $srow->$_ } @$list;

    @columns || Carp::croak 'select requires columns';

    my $expr;
    if ( my @keys = keys %$where ) {
        $expr = _expr_join( ' AND ', map { $srow->$_ == $where->{$_} } @keys );
    }

    return $self->fetch(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    ) if wantarray;

    return $self->fetch1(
        select => \@columns,
        from   => $srow,
        $expr ? ( where => $expr ) : (),
    );
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

1;
