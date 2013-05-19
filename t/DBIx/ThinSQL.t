use strict;
use warnings;
use lib 't/lib';
use DBIx::ThinSQL ':default', ':sql';
use Test::DBIx::ThinSQL qw/run_in_tempdir/;
use Test::Fatal qw/exception/;
use Test::More;

subtest "DBIx::ThinSQL::_bv", sub {
    my $bv = DBIx::ThinSQL::_bv->new( 1, 2 );
    isa_ok $bv, 'DBIx::ThinSQL::_bv';
    is $bv->val,  1, 'bv value match';
    is $bv->type, 2, 'bv type match';
    is_deeply [ $bv->for_bind_param ], [ 1, 2 ], 'for_bind_param';

    $bv = DBIx::ThinSQL::_bv->new(1);
    isa_ok $bv, 'DBIx::ThinSQL::_bv';
    is $bv->val,  1,     'bv value match';
    is $bv->type, undef, 'bv type match';
    is_deeply [ $bv->for_bind_param ], [1], 'for_bind_param undef';

    my $qv = DBIx::ThinSQL::_qv->new(1);
    is( DBIx::ThinSQL::_bv->new($qv), $qv, 'qv passthrough' );
};

subtest "DBIx::ThinSQL::_qv", sub {
    my $qv = DBIx::ThinSQL::_qv->new( 1, 2 );
    isa_ok $qv, 'DBIx::ThinSQL::_qv';
    is $qv->val,  1, 'qv value match';
    is $qv->type, 2, 'qv type match';
    is_deeply [ $qv->for_quote ], [ 1, 2 ], 'for_quote';

    $qv = DBIx::ThinSQL::_qv->new(1);
    isa_ok $qv, 'DBIx::ThinSQL::_qv';
    is $qv->val,  1,     'qv value match';
    is $qv->type, undef, 'qv type match';
    is_deeply [ $qv->for_quote ], [1], 'for_quote undef';

    my $bv = DBIx::ThinSQL::_bv->new(1);
    is( DBIx::ThinSQL::_qv->new($bv), $bv, 'bv passthrough' );
};

subtest "DBIx::ThinSQL::_qi", sub {
    my $qi = DBIx::ThinSQL::_qi->new('name');
    isa_ok $qi, 'DBIx::ThinSQL::_qi';
    is $qi->val, 'name', 'qi value match';
};

subtest "DBIx::ThinSQL::_expr", sub {

    my $bv = bv(1);
    my $qv = qv(2);

    # _expr
    my $expr = DBIx::ThinSQL::_expr->new( 'func', 1, $qv, $bv );
    isa_ok $expr, 'DBIx::ThinSQL::_expr';

    my $func = DBIx::ThinSQL::func( 'sum', ', ', 1, $qv, $bv );
    isa_ok $func, 'DBIx::ThinSQL::_expr', 'func';

    is_deeply [ $func->tokens ], [ 'SUM', '(', 1, ', ', $qv, ', ', $bv, ')' ],
      'expr tokens';

    my $func_as = $func->as('name');
    isa_ok $func_as, 'DBIx::ThinSQL::_expr', 'func->as returns _expr';

    my @tokens = $func_as->tokens;
    my $qi     = $tokens[9];
    isa_ok $qi, 'DBIx::ThinSQL::_qi';
    is_deeply [ $func_as->tokens ],
      [ 'SUM', '(', 1, ', ', $qv, ', ', $bv, ')', ' AS ', $qi ],
      'func_as->tokens';
};

subtest "DBIx::ThinSQL::st", sub {
    can_ok 'DBIx::ThinSQL::st', qw/
      array
      arrays
      hash
      hashes
      /;
};

subtest "DBIx::ThinSQL", sub {

    # check exported functions
    isa_ok \&bv,  'CODE', 'export bv';
    isa_ok \&qv,  'CODE', 'export qv';
    isa_ok \&AND, 'CODE', 'export AND';
    isa_ok \&OR,  'CODE', 'export OR';

    subtest 'internal', sub {

        # _ejoin
        my @tokens = DBIx::ThinSQL::_ejoin('a');
        ok !@tokens, 'ejoin nothing';

        @tokens = DBIx::ThinSQL::_ejoin(qw/ a 1 /);
        is_deeply \@tokens, [1], 'ejoin one';

        @tokens = DBIx::ThinSQL::_ejoin(qw/ a 1 2 3 4 /);
        is_deeply \@tokens, [qw/ 1 a 2 a 3 a 4 /], 'ejoin many';

        my $bv = bv(1);
        my $qv = qv(2);

        @tokens = DBIx::ThinSQL::_ejoin( qw/ a 1 /, $bv, $qv );
        is_deeply \@tokens, [ qw/ 1 a /, $bv, qw/ a /, $qv ], 'ejoin many bqv';

        # case
        my $case = case (
            when => 1,
            then => $qv,
            else => $bv,
        );
        isa_ok $case, 'DBIx::ThinSQL::_expr';

        my @sql = $case->tokens;
        like "@sql", qr/CASE \s WHEN \s+ 1 \s+ THEN \s+
            DBIx::ThinSQL::_qv.* \s+ ELSE \s+ DBIx::ThinSQL::_bv.* \s+
            END/sx, 'CASE';

        @sql = cast( 'col AS', 'integer' )->as('icol')->tokens;
        like "@sql", qr/CAST \s+ \( \s+ col \s AS \s+ integer \s+ \)
            \s+ AS \s+ DBIx::ThinSQL::_qi.* /sx, 'CAST';

        my $concat = concat( 1, $qv, $bv );
        isa_ok $concat, 'DBIx::ThinSQL::_expr';

        @sql = $concat->tokens;
        like "@sql", qr/1 \s+ || \s+ DBIx::ThinSQL::_qv.* \s+
            || \s+ DBIx::ThinSQL::_bv.*\s+$/sx, 'CONCAT';

    };

    # now let's make a database check our syntax
    run_in_tempdir {

        my $driver = eval { require DBD::SQLite } ? 'SQLite' : 'DBM';

        # DBD::DBM doesn't seem to support this style of construction
      SKIP: {
            skip 'DBD::DBM limitation', 1 if $driver eq 'DBM';

            my $db = DBI->connect(
                "dbi:$driver:dbname=x",
                '', '',
                {
                    RaiseError => 1,
                    PrintError => 0,
                    RootClass  => 'DBIx::ThinSQL',
                },
            );

            isa_ok $db, 'DBIx::ThinSQL::db';
        }

        my $db =
          DBIx::ThinSQL->connect( "dbi:$driver:dbname=x'", '', '',
            { RaiseError => 1, PrintError => 0, },
          );

        isa_ok $db, 'DBIx::ThinSQL::db';

        $db->do("CREATE TABLE users ( name TEXT PRIMARY KEY, phone TEXT )");
        my $res;
        my @res;

        $res = $db->xdo(
            insert_into => 'users',
            values      => [ 'name1', bv('phone1') ],
        );
        is $res, 1, 'xdo insert 1';

        $res = $db->xdo(
            insert_into => 'users',
            values      => [ bv('name2'), 'phone4' ],
        );
        is $res, 1, 'xdo insert 2';

        $res = $db->xdo(
            insert_into => 'users',
            values      => [ 'name3', 'phone3' ],
        );
        is $res, 1, 'xdo insert 3';

        $res = $db->xdo(
            delete_from => 'users',
            where       => [ 'name = ', bv('name3') ],
        );
        is $res, 1, 'xdo delete 3';

        $res = $db->xdo(
            update => 'users',
            set    => [ 'phone = ', bv('phone2') ],
            where  => [ 'name = ', bv('name2') ],
        );
        is $res, 1, 'xdo update 2';

        subtest 'xarray', sub {
            $res = $db->xarray(
                select   => 'name, phone',
                from     => 'users',
                order_by => 'name',
            );

            is_deeply $res, [ 'name1', 'phone1' ], 'xarray scalar';

            $res = $db->xarray(
                select => 'name, phone',
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply $res, undef, 'xarray scalar undef';

            @res = $db->xarray(
                select   => [qw/name phone/],
                from     => 'users',
                order_by => 'name desc',
            );

            is_deeply \@res, [ 'name2', 'phone2' ], 'xarray list';

            @res = $db->xarray(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply \@res, [], 'xarray list undef';
        };

        subtest 'xarrays', sub {
            $res = $db->xarrays(
                select   => [qw/name phone/],
                from     => 'users',
                group_by => [qw/name phone/],
                order_by => 'name asc',
            );

            is_deeply $res, [ [qw/name1 phone1/], [qw/name2 phone2/] ],
              'xarrays scalar';

            $res = $db->xarrays(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply $res, [], 'xarrays scalar undef';

            @res = $db->xarrays(
                select   => [qw/name phone/],
                from     => 'users',
                order_by => 'name desc',
            );

            is_deeply \@res, [ [qw/name2 phone2/], [qw/name1 phone1/] ],
              'xarrays list';

            @res = $db->xarrays(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply \@res, [], 'xarrays list undef';

        };

        subtest 'xhash', sub {
            $res = $db->xhash(
                select   => [qw/name phone/],
                from     => 'users',
                order_by => 'name desc',
            );

            is_deeply $res, { name => 'name2', phone => 'phone2' },
              'xhash scalar';

            $res = $db->xhash(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply $res, undef, 'xhash scalar undef';

        };

        subtest 'xhashes', sub {
            $res = $db->xhashes(
                select   => [qw/name phone/],
                from     => 'users',
                order_by => 'name asc',
            );

            is_deeply $res,
              [
                { name => 'name1', phone => 'phone1', },
                { name => 'name2', phone => 'phone2', },
              ],
              'xhashes scalar';

            $res = $db->xhashes(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply $res, [], 'xhashes scalar undef';

            @res = $db->xhashes(
                select   => [qw/name phone/],
                from     => 'users',
                order_by => 'name desc',
            );

            is_deeply \@res,
              [
                { name => 'name2', phone => 'phone2' },
                { name => 'name1', phone => 'phone1' },
              ],
              'xhashes list';

            @res = $db->xhashes(
                select => [qw/name phone/],
                from   => 'users',
                where  => 'name IS NULL',
            );

            is_deeply \@res, [], 'xhashes list undef';
        };

      SKIP: {
            skip 'DBD::DBM limitation', 2 if $driver eq 'DBM';
            $res = $db->xdo(
                insert_into => 'users(name, phone)',
                select      => [ qv('name3'), qv('phone3') ],
            );
            is $res, 1, 'insert into select';

            $res = $db->xarrays(
                select => [qw/name phone/],
                from   => 'users',
                where =>
                  [ 'phone = ', bv('phone3'), 'OR name = ', qv('name2') ],
                order_by => [ 'phone', 'name' ],
            );

            is_deeply $res, [ [qw/name2 phone2/], [qw/name3 phone3/] ], 'where';
        }

        $res = $db->xdo(
            insert_into => 'users',
            values      => { name => 'name4', phone => 'phone4' },
        );
        is $res, 1, 'insert using hashref';

        $db->do('DELETE FROM users');

        subtest 'txn', sub {
            $res = undef;
            ok $db->{AutoCommit}, 'have autocommit';

            $db->txn(
                sub {
                    ok !$db->{AutoCommit}, 'no autocommit in txn';
                    $res = 1;
                }
            );

            ok $db->{AutoCommit}, 'have autocommit';
            is $res, 1, 'sub ran in txn()';

            $res = undef;
            like exception {
                $db->txn(
                    sub {
                        die 'WTF';
                    }
                );
                die "WRONG";
            }, qr/WTF/, 'correct exception propagated';

            is $db->{private_DBIx_ThinSQL_txn}, 0, 'txn 0';

            $res = $db->txn(
                sub {
                    $db->txn(
                        sub {
                            $db->xdo(
                                insert_into => 'users',
                                values =>
                                  { name => 'name1', phone => 'phone1' },
                            );

                            $res = $db->xarrays(
                                select => [qw/name phone/],
                                from   => 'users',
                            );
                        }
                    );
                }
            );

            is_deeply $res, [ [qw/name1 phone1/] ], 'nested txn';
            is $db->{private_DBIx_ThinSQL_txn}, 0, 'txn 0';

            my $err;
            @res = $db->txn(
                sub {
                    eval {
                        $db->txn(
                            sub {
                                $db->xdo(
                                    insert_into => 'users',
                                    values =>
                                      { name => 'name1', phone => 'phone1' },
                                );
                            }
                        );
                    };

                    $err = $@;

                    $db->xdo(
                        insert_into => 'users',
                        values      => { name => 'name2', phone => 'phone2' },
                    );

                    return $db->xarrays(
                        select   => [qw/name phone/],
                        from     => 'users',
                        order_by => 'name',
                    );
                }
            );

            ok $err, 'know that duplicate insert failed';
            is_deeply \@res, [ [qw/name1 phone1/], [qw/name2 phone2/] ],
              'nested txn/svp';
            is $db->{private_DBIx_ThinSQL_txn}, 0, 'txn 0';

        };

        $db->disconnect;

    }
};

done_testing();
