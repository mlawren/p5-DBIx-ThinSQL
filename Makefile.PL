use inc::Module::Install;

name 'DBIx-ThinSQL';
version '0.0.41_1';
license 'gpl3';

homepage 'https://github.com/mlawren/p5-DBIx-ThinSQL';
bugtracker 'https://github.com/mlawren/p5-DBIx-ThinSQL/issues';
repository 'git://github.com/mlawren/p5-DBIx-ThinSQL.git';

all_from 'lib/DBIx/ThinSQL.pm';
readme_from 'lib/DBIx/ThinSQL.pm';

requires 'DBI'            => 0;
requires 'Exporter::Tidy' => 0;
requires 'File::ShareDir' => 0;
requires 'Log::Any'       => 0;
requires 'Path::Tiny'     => 0;

test_requires 'File::chdir'    => 0;
test_requires 'Test::Database' => 0;
test_requires 'Test::Fatal'    => 0;
test_requires 'Test::More'     => 0;

tests_recursive 't';
author_tests 'xt';

install_share 'share';

no_index package => 'DBIx::ThinSQL::Driver';
no_index package => 'DBIx::ThinSQL::Driver::Pg';
no_index package => 'DBIx::ThinSQL::Driver::SQLite';
no_index package => 'DBIx::ThinSQL::_bv';
no_index package => 'DBIx::ThinSQL::_expr';
no_index package => 'DBIx::ThinSQL::_qi';
no_index package => 'DBIx::ThinSQL::_qv';
no_index package => 'DBIx::ThinSQL::db';
no_index package => 'DBIx::ThinSQL::st';

auto_install;

WriteAll;
