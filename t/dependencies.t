#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;

use_ok('AnyEvent::Graphite');
use_ok('DateTime');
use_ok('DateTime::Duration');
use_ok('Genome');

done_testing();
