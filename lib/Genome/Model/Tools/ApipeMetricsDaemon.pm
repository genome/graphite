package Genome::Model::Tools::ApipeMetricsDaemon;

use strict;
use warnings;

use lib '/gscuser/nnutter/lib/gsc_bin_perl_5.8.7/lib/perl5';
use AnyEvent;
use AnyEvent::Graphite;
use DateTime;
use Genome;
use Log::Log4perl qw(:easy);


class Genome::Model::Tools::ApipeMetricsDaemon {
    is => 'Command::V2',
    doc => 'apipe-metrics-daemon collects data periodically and reports it to graphite',
    has => [
        log_file => {
            is => 'Text',
            doc => 'path to log file',
            default => '/var/log/apipe-metrics-daemon/apipe-metrics-daemon.log',
        },
        log_level => {
            is => 'Text',
            doc => 'log level',
            valid_values => ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL', 'ALL', 'OFF'],
            default => 'INFO',
        },
        graphite_host => {
            is => 'Text',
            doc => 'graphite host',
            default => '10.0.28.195',
        },
        graphite_port => {
            is => 'Integer',
            doc => 'graphite port',
            default => 2003,
        },
    ],
    has_optional => [
        _dbh => {
            is => 'Text',
            doc => 'database handle'
        },
        _graphite => {
            is => 'Text',
            doc => 'graphite handle'
        },
        _logger => {
            is => 'Text',
            doc => 'logger handle'
        },
    ],
};


sub help_synopsis {
    return 'apipe-metrics-daemon collects data periodically and reports it to graphite' . "\n";
}


sub help_detail {
    return <<EOS
apipe-metrics-daemon collects data periodically and reports it to graphite running internally on apipe-metrics.gsc.wustl.edu
The source is available at (https://github.com/genome/graphite) and via a submodule of Genome.git.
EOS
}


sub execute {
    my $self = shift;
    $self->init();
    $self->start_daemon();
    $self->cleanup();
    return 1;
}


sub init {
    my $self = shift;

    $self->_logger($self->init_logger);
    die "Failed to initialize logger.\n" unless $self->_logger->isa('Log::Log4perl::Logger');

    $self->_graphite(AnyEvent::Graphite->new(host => $self->graphite_host, port => $self->graphite_port));
    die "Failed to get Graphite connection.\n" unless $self->_graphite->isa('AnyEvent::Graphite');

    $self->db_connect();
    unless ($self->_dbh) {
        $self->cleanup();
        die "Failed to get DB connection.\n";
    }
    return 1;
}


sub init_logger {
    my $self = shift;
    (my $category = __PACKAGE__) =~ s/\:\:/./g;
    my $log_file = $self->log_file;
    my $log_level = $self->log_level;
    my @conf = (
        "log4perl.category.$category = $log_level, Logfile",
        'log4perl.appender.Logfile = Log::Log4perl::Appender::File',
        "log4perl.appender.Logfile.filename = $log_file",
        'log4perl.appender.Logfile.layout = Log::Log4perl::Layout::PatternLayout',
        'log4perl.appender.Logfile.layout.ConversionPattern = %d %m %n',
    );
    my $conf = join("\n", @conf);
    Log::Log4perl::init(\$conf);
    my $logger = Log::Log4perl->get_logger(__PACKAGE__);
    return $logger;
}


sub start_daemon {
    my $self = shift;
    $self->_logger->info('Starting daemon...');
    $self->_logger->info('Metrics will be sent to ' . $self->graphite_host . ':' . $self->graphite_port . '...');
    my $now      = DateTime->now(time_zone => 'America/Chicago');
    my $hours    = DateTime::Duration->new(hours   => $now->hour);
    my $minutes  = DateTime::Duration->new(minutes => $now->minute);
    my $seconds  = DateTime::Duration->new(seconds => $now->second);

    my $hour   = DateTime::Duration->new(hours => 1);
    my $day    = DateTime::Duration->new(days  => 1);
    my $minute = DateTime::Duration->new(minutes => 1);

    # docs say time zone should be UTC in order to add/subtract correctly
    $now->set_time_zone('UTC');

    my $next_hour = $now - $minutes - $seconds + $hour;
    my $next_day  = $now - $hours - $minutes - $seconds + $day;
    my $next_min  = $now - $seconds + $minute;

    my $day_delay  = $next_day->subtract_datetime_absolute($now)->seconds;
    my $hour_delay = $next_hour->subtract_datetime_absolute($now)->seconds;
    my $min_delay  = $next_min->subtract_datetime_absolute($now)->seconds;

    my $exit_program = AnyEvent->signal(signal => "INT", cb => sub { $self->cleanup; exit 255 });
    my $done = AnyEvent->condvar;
    my $every_day    = AnyEvent->timer(after => $day_delay,  interval => 86400, cb => sub { $self->every_day });
    my $every_hour   = AnyEvent->timer(after => $hour_delay, interval => 3600,  cb => sub { $self->every_hour });
    my $every_minute = AnyEvent->timer(after => $min_delay,  interval => 60,    cb => sub { $self->every_minute });
    $done->recv;
}


sub graphite_send {
    my $self = shift;
    my $metric = shift;
    my ($name, $value, $timestamp) = $self->$metric;
    my $log_name = $name . ' 'x(50 - length($name));
    my $log_value = $value . ' 'x(15 - length($value));
    $self->_logger->info(join("\t", $log_name, $log_value, $timestamp));
    return $self->_graphite->send($name, $value, $timestamp);
}


sub db_connect {
    my $self = shift;
    my $dbh = Genome::DataSource::GMSchema->get_default_handle();
    $self->_dbh($dbh);
    die "Failed to connect to the database!\n" unless $self->_dbh;
    return 1;
}


sub cleanup {
    my $self = shift;
    $self->_dbh->rollback if ($self->_dbh);
    $self->_dbh->disconnect if ($self->_dbh);
    $self->_graphite->finish if ($self->_graphite);
}


sub parse_sqlrun_count {
    my $sql = shift;
    my $output = qx{sqlrun --instance=warehouse "$sql" | head -n 3 | tail -n 1};
    my ($value) = $output =~ /^(\d+)/;
    return $value;
}

###################
#### Every Day ####
###################

sub every_day {
    my $self = shift;
    $self->_logger->info('every_day');
    $self->graphite_send('builds_daily_failed');
    $self->graphite_send('builds_daily_succeeded');
    $self->graphite_send('builds_daily_unstartable');
    return 1;
}

sub builds_prior_daily_status {
    my $status = shift;

    my $datetime = DateTime->now(time_zone => 'America/Chicago');
    my $timeshift = DateTime::Duration->new(days => 1, hours => $datetime->hour, minutes => $datetime->minute, seconds => $datetime->second);
    $datetime -= $timeshift; # Looking at the last complete day

    my $name = join('.', 'builds', 'daily_' . lc($status));
    my $timestamp = $datetime->strftime("%s");

    my $date_completed = $datetime->strftime('%F');
    my @builds = Genome::Model::Build->get(
        run_by => 'apipe-builder',
        status => $status,
        'date_completed like' => "$date_completed %",
    );
    my $value = scalar @builds;

    return ($name, $value, $timestamp);
}
sub builds_daily_failed {
    return builds_prior_daily_status('Failed');
}
sub builds_daily_succeeded {
    return builds_prior_daily_status('Succeeded');
}
sub builds_daily_unstartable {
    return builds_prior_daily_status('Unstartable');
}

####################
#### Every Hour ####
####################

sub every_hour {
    my $self = shift;
    $self->_logger->info('every_hour');
    $self->graphite_send('builds_hourly_failed');
    $self->graphite_send('builds_hourly_succeeded');
    $self->graphite_send('builds_hourly_unstartable');
    return 1;
}

sub builds_prior_hour_status {
    my $status = shift;

    my $datetime = DateTime->now(time_zone => 'America/Chicago');
    my $timeshift = DateTime::Duration->new(hours => 1, minutes => $datetime->minute, seconds => $datetime->second);
    $datetime -= $timeshift; # Looking at the last complete hour

    my $name = join('.', 'builds', 'hourly_' . lc($status));
    my $timestamp = $datetime->strftime("%s");

    my $date_completed = $datetime->strftime('%F %H:');
    my @builds = Genome::Model::Build->get(
        run_by => 'apipe-builder',
        status => $status,
        'date_completed like' => "$date_completed%",
    );
    my $value = scalar @builds;

    return ($name, $value, $timestamp);
}
sub builds_hourly_failed {
    return builds_prior_hour_status('Failed');
}
sub builds_hourly_succeeded {
    return builds_prior_hour_status('Succeeded');
}
sub builds_hourly_unstartable {
    return builds_prior_hour_status('Unstartable');
}

######################
#### Every Minute ####
######################

sub every_minute {
    my $self = shift;
    $self->_logger->info('every_minute');
    $self->graphite_send('builds_current_failed');
    $self->graphite_send('builds_current_running');
    $self->graphite_send('builds_current_scheduled');
    $self->graphite_send('builds_current_succeeded');
    $self->graphite_send('builds_current_unstartable');
    $self->graphite_send('lsf_workflow_run');
    $self->graphite_send('lsf_workflow_pend');
    $self->graphite_send('lsf_alignment_run');
    $self->graphite_send('lsf_alignment_pend');
    $self->graphite_send('lsf_blades_run');
    $self->graphite_send('lsf_blades_pend');
    $self->graphite_send('models_build_requested');
    $self->graphite_send('models_build_requested_first_build');
    $self->graphite_send('models_buildless');
    $self->graphite_send('models_failed');
    return 1;
}

sub builds_current_status {
    my $status = shift;
    my $name = join('.', 'builds', 'current_' . lc($status));
    my $timestamp = DateTime->now->strftime("%s");
    my $value = parse_sqlrun_count("select count(distinct(gm.genome_model_id)) from mg.genome_model gm where exists (select * from mg.genome_model_build gmb where gmb.model_id = gm.genome_model_id and exists (select * from mg.genome_model_event gme where gme.event_type = 'genome model build' and gme.build_id = gmb.build_id and gme.event_status = '$status' and gme.user_name = 'apipe-builder'))");
    return ($name, $value, $timestamp);
}
sub builds_current_failed {
    return builds_current_status('Failed');
}
sub builds_current_running {
    return builds_current_status('Running');
}
sub builds_current_scheduled {
    return builds_current_status('Scheduled');
}
sub builds_current_succeeded {
    return builds_current_status('Succeeded');
}
sub builds_current_unstartable {
    return builds_current_status('Unstartable');
}

sub lsf_queue_status {
    my ($queue, $status) = @_;
    my $name = join('.', 'lsf', $queue, lc($status));
    my $timestamp = DateTime->now->strftime("%s");
    my $bjobs_output = qx(bjobs -u apipe-builder -q $queue 2> /dev/null | grep ^[0-9] | grep $status | wc -l);
    my ($value) = $bjobs_output =~ /^(\d+)/;
    return ($name, $value, $timestamp);
}
sub lsf_workflow_run {
    return lsf_queue_status('workflow', 'RUN');
}
sub lsf_workflow_pend {
    return lsf_queue_status('workflow', 'PEND');
}
sub lsf_alignment_run {
    return lsf_queue_status('alignment-pd', 'RUN');
}
sub lsf_alignment_pend {
    return lsf_queue_status('alignment-pd', 'PEND');
}
sub lsf_blades_status {
    my $status = shift;
    my ($long_name, $long_value, $long_timestamp) = lsf_queue_status('long', $status);
    my ($apipe_name, $apipe_value, $apipe_timestamp) = lsf_queue_status('apipe', $status);
    (my $name = $long_name) =~ s/long/blades/g;
    my $timestamp = $long_timestamp;
    my $value = $long_value + $apipe_value;
    return ($name, $value, $timestamp);
}
sub lsf_blades_run {
    return lsf_blades_status('RUN');
}
sub lsf_blades_pend {
    return lsf_blades_status('PEND');
}

sub models_build_requested {
    my $name = join('.', 'models', 'build_requested');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = parse_sqlrun_count("select count(*) from mg.genome_model gm where gm.build_requested = 1");
    return ($name, $value, $timestamp);
}
sub models_build_requested_first_build {
    my $name = join('.', 'models', 'build_requested_first_build');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = parse_sqlrun_count("select count(*) from mg.genome_model gm where gm.build_requested = 1 and not exists (select * from mg.genome_model_build gmb where gmb.model_id = gm.genome_model_id)");
    return ($name, $value, $timestamp);
}
sub models_buildless {
    my $name = join('.', 'models', 'buildless');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = parse_sqlrun_count("select count(*) from mg.genome_model gm where gm.build_requested != 1 and gm.user_name = 'apipe-builder' and not exists (select * from mg.genome_model_build gmb where gmb.model_id = gm.genome_model_id)");
    return ($name, $value, $timestamp);
}
sub models_failed {
    my $name = join('.', 'models', 'failed');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = parse_sqlrun_count("select count(distinct(gm.genome_model_id)) from mg.genome_model gm where exists (select * from mg.genome_model_build gmb where gmb.model_id = gm.genome_model_id and exists (select * from mg.genome_model_event gme where gme.event_type = 'genome model build' and gme.build_id = gmb.build_id and gme.event_status = 'Failed' and gme.user_name = 'apipe-builder'))");
    return ($name, $value, $timestamp);
}
