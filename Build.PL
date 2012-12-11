#!/usr/bin/perl
# Use local perl, not some perl on an application server!

use Config;
use Module::Build;

BEGIN {
  unshift @INC, "$ENV{PWD}/blib/lib";
}

my $class = Module::Build->subclass (
    class => 'Pod::Builder',
    code => <<'EOS',

      sub ACTION_clean {
        # FIXME: is this safe?
        use File::Path qw/rmtree/;
        rmtree "./_build";
        rmtree "./blib";
        rmtree "./cmd-bindoc";
        unlink "./Build";
        unlink "./MYMETA.yml";
      }
EOS
);

my $build = $class->new(
  module_name => 'Genome::Model::Tools::ApipeMetricsDaemon',
  license => 'lgpl',
  dist_version => '1.1.14',
  dist_abstract => 'collect and report metrics to graphite',
);

$build->create_build_script;
