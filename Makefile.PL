use inc::Module::Install;

name 'DBIx-ThinSQL';
version '0.0.1_1';
license 'gpl3';
repository 'http://github.com/mlawren/p5-DBIx-ThinSQL/';
all_from 'lib/DBIx/ThinSQL.pm';
readme_from 'lib/DBIx/ThinSQL.pod';

requires 'DBI'            => 0;
requires 'Exporter::Tidy' => 0;
requires 'File::ShareDir' => 0;
requires 'Log::Any'       => 0;
requires 'Path::Tiny'     => 0;

test_requires 'File::chdir'    => 0;
test_requires 'Test::Database' => 0;
test_requires 'Test::More'     => 0;

tests_recursive 't';
author_tests 'xt';

install_share 'share';

auto_install;

WriteAll;
