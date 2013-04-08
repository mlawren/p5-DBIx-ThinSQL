use strict;
use warnings;
use lib 't/lib';
use Test::More;
use DBIx::ThinSQL qw/bv qv OR AND/;
use Test::DBIx::ThinSQL qw/run_in_tempdir/;

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
    isa_ok bv( 1, 2 ), 'DBIx::ThinSQL::_bv';
    isa_ok qv(1), 'DBIx::ThinSQL::_qv';
    is OR,  ' OR ',  'OR exported';
    is AND, ' AND ', 'AND exported';

    # This one is private, but I had to test it while writing ...
    is_deeply [ DBIx::ThinSQL::_ljoin(qw/a/) ], [], 'ljoin nothing';

    is_deeply [ DBIx::ThinSQL::_ljoin(qw/a 1/) ], [qw/1/], 'ljoin one';

    is_deeply [ DBIx::ThinSQL::_ljoin(qw/a 1 2 3 4/) ], [qw/1 a 2 a 3 a 4/],
      'ljoin many';

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

        $db->do("CREATE TABLE users ( name TEXT, phone TEXT )");
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
                  [ 'phone = ', bv('phone3'), OR, 'name = ', qv('name2') ],
                order_by => [ 'phone', 'name' ],
            );

            is_deeply $res, [ [qw/name2 phone2/], [qw/name3 phone3/] ], 'where';
        }

        $res = $db->xdo(
            insert_into => 'users',
            values      => { name => 'name4', phone => 'phone4' },
        );
        is $res, 1, 'insert using hashref';

        $db->disconnect;

    }
};

done_testing();
