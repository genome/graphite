package Genome::Model::Tools::ApipeMetricsDaemon;

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Graphite;
use DateTime;
use Genome;
use Log::Log4perl qw(:easy);
use Logfile::Rotate;


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
            doc => 'graphite host, specify 0.0.0.0 to disable',
            default => 'localhost',
        },
        graphite_port => {
            is => 'Integer',
            doc => 'graphite port',
            default => 2003,
        },
    ],
    has_optional => [
        _data_sources => {
            is => 'Genome::DataSource',
            doc => 'database handles',
            is_many => 1,
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

    if ($self->graphite_host ne '0.0.0.0') {
        $self->_graphite(AnyEvent::Graphite->new(host => $self->graphite_host, port => $self->graphite_port));
        die "Failed to get Graphite connection.\n" unless $self->_graphite->isa('AnyEvent::Graphite');
    }

    return 1;
}


sub rotate_log {
    my $self = shift;

    my $log_file = $self->log_file;
    my $log = new Logfile::Rotate(
        File => $log_file, 
        Count => 5,
    );

    unless ($log->rotate) {
        die "Failed to rotate log file ($log_file).\n";
    }

    return 1;
}


sub init_logger {
    my $self = shift;

    (my $category = __PACKAGE__) =~ s/\:\:/./g;
    my $log_level = $self->log_level;

    my $log_file = $self->log_file;
    $self->rotate_log if (-e $log_file && -s $log_file > 104_857_600) ;

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

    if ($self->graphite_host ne '0.0.0.0') {
        $self->_logger->info('Metrics will be sent to ' . $self->graphite_host . ':' . $self->graphite_port . '.');
    }
    else {
        $self->_logger->info('Metrics will not be sent to Graphite.');
    }

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

sub log_metric {
    my $self = shift;
    my @metric_data = @_;

    # Sending to Graphite can be disabled by specifying 0.0.0.0 as the Graphite host.
    # So we don't want to die if it doesn't exist but we do need to assume it may not exist.
    my $graphite = $self->_graphite;

    while (@metric_data) {
        my $name = shift @metric_data;
        my $value = shift @metric_data;
        my $timestamp = shift @metric_data;
        my $log_name = $name . ' 'x(50 - length($name));
        my $log_value = $value . ' 'x(15 - length($value));
        $self->_logger->info(join("\t", $log_name, $log_value, $timestamp));
        if ($graphite) {
            my $prefixed_name = 'apipe.' . $name;
            $graphite->send($prefixed_name, $value, $timestamp); # how do we check for success asynchronously?
        }
    }
    return 1;
}


sub cleanup {
    my $self = shift;
    my @data_sources = $self->_data_sources;
    for my $data_source (@data_sources) {
        $data_source->disconnect_default_handle if $data_source->has_default_handle;
    }
    $self->_graphite->finish if ($self->_graphite);
}


sub get_data_source {
    my $self = shift;
    my $data_source_class = shift;

    my ($data_source) = grep { $_->isa($data_source_class) } $self->_data_sources;
    unless ($data_source) {
        $self->_logger->info("Connecting to $data_source_class...");
        $data_source = $data_source_class->get();
        $self->add__data_source($data_source);
    }
    die "Unable to get database handle to $data_source_class.\n" unless $data_source;

    return $data_source;
}


sub parse_sqlrun_count {
    my $self = shift;
    my $sql = shift;

    # default to GMSchema but allow override
    my $data_source_class = 'Genome::DataSource::GMSchema';
    $data_source_class = shift if (@_);

    my $data_source = $self->get_data_source($data_source_class);
    my $dbh = $data_source->get_default_handle;
    my $results = $dbh->selectcol_arrayref($sql);
    die $dbh->errstr if not $results;

    return $results->[0];
}

sub eval_metrics {
    my $self = shift;
    my @m    = @_;

    for my $m (@m) {
        my @metrics = eval { $self->$m };
        if (my $error = $@) {
            $self->_logger->error(sprintf('%s: %s', $m, $error));
        }
        if (@metrics) {
            $self->log_metric(@metrics);
        }
    }
}

###################
#### Every Day ####
###################

sub every_day {
    my $self = shift;
    $self->_logger->info('every_day');
    $self->eval_metrics(qw(builds_daily_failed builds_daily_succeeded builds_daily_unstartable));
    return 1;
}

sub builds_prior_daily_status {
    my $self = shift;
    my $status = shift;

    my $datetime = DateTime->now(time_zone => 'America/Chicago');
    my $timeshift = DateTime::Duration->new(days => 1, hours => $datetime->hour, minutes => $datetime->minute, seconds => $datetime->second);
    $datetime -= $timeshift; # Looking at the last complete day

    my $name = join('.', 'builds', 'daily_' . lc($status));
    my $timestamp = $datetime->strftime("%s");

    my $date_completed = $datetime->strftime('%F');
    my @builds = Genome::Model::Build->get(
        run_by => ['apipe-builder', 'prod-builder'],
        status => $status,
        'date_completed >' => $date_completed,
    );
    my $value = scalar @builds;

    return ($name, $value, $timestamp);
}
sub builds_daily_failed {
    my $self = shift;
    return $self->builds_prior_daily_status('Failed');
}
sub builds_daily_succeeded {
    my $self = shift;
    return $self->builds_prior_daily_status('Succeeded');
}
sub builds_daily_unstartable {
    my $self = shift;
    return $self->builds_prior_daily_status('Unstartable');
}

####################
#### Every Hour ####
####################

sub every_hour {
    my $self = shift;
    $self->_logger->info('every_hour');
    $self->eval_metrics(qw(builds_hourly_failed builds_hourly_succeeded builds_hourly_unstartable genome_test_tracker_time));
    return 1;
}

sub genome_test_tracker_time {
    my $user = $ENV{GENOMEX_DS_TEST_TRACKER_LOGIN};
    my $pass = $ENV{GENOMEX_DS_TEST_TRACKER_AUTH};
    my $dsn = 'dbi:Pg:' . ($ENV{GENOMEX_DS_TEST_TRACKER_SERVER} || '');

    my $dbh = DBI->connect($dsn, $user, $pass, { PrintError => 0 });
    unless ($dbh) {
        return;
    }

    my $sql = 'select sum(duration), max(duration) from test_tracker.test';
    my $results = $dbh->selectrow_arrayref($sql);
    unless ($results) {
        die $dbh->errstr;
    }

    my $timestamp = DateTime->now->strftime("%s");
    return (
        'test_tracker.genome.total_time', $results->[0], $timestamp,
        'test_tracker.genome.max_time',   $results->[1], $timestamp,
    );
}

sub builds_prior_hour_status {
    my $self = shift;
    my $status = shift;

    my $datetime = DateTime->now(time_zone => 'America/Chicago');
    my $timeshift = DateTime::Duration->new(hours => 1, minutes => $datetime->minute, seconds => $datetime->second);
    $datetime -= $timeshift; # Looking at the last complete hour

    my $name = join('.', 'builds', 'hourly_' . lc($status));
    my $timestamp = $datetime->strftime("%s");

    my $date_completed = $datetime->strftime('%F %H:00:00');
    my @builds = Genome::Model::Build->get(
        run_by => ['apipe-builder', 'prod-builder'],
        status => $status,
        'date_completed >' => $date_completed,
    );
    my $value = scalar @builds;

    return ($name, $value, $timestamp);
}
sub builds_hourly_failed {
    my $self = shift;
    return $self->builds_prior_hour_status('Failed');
}
sub builds_hourly_succeeded {
    my $self = shift;
    return $self->builds_prior_hour_status('Succeeded');
}
sub builds_hourly_unstartable {
    my $self = shift;
    return $self->builds_prior_hour_status('Unstartable');
}

######################
#### Every Minute ####
######################

sub every_minute {
    my $self = shift;
    $self->_logger->info('every_minute');

    # Build metrics
    $self->build_status_by_user(
        status => ['New', 'Failed', 'Running', 'Scheduled', 'Succeeded', 'Unstartable'],
        user => ['prod-builder', 'apipe-builder', 'all'],
    );

    $self->model_status_by_user(
        status => ['Running', 'Scheduled', 'Failed', 'Unstartable'],
        user => ['prod-builder', 'apipe-builder', 'all'],
    );

    $self->pipeline_metrics_by_processing_profile(
        reference_alignment => {
            2580856 => 'feb_2011_default',
            2581081 => 'feb_2011_default_lane_qc',
            2635769 => 'nov_2011_default',
            2653572 => 'nov_2011_default_lane_qc',
        },
        de_novo_assembly => {
            2495849 => 'velvet_solexa',
            2498894 => 'velvet_solexa_bwa_qual_10_filter_35',
            2509648 => 'velvet_illumina_bwa_kmer_range_31_35',
            2569540 => 'dacc_imported_soap',
            2599969 => 'velvet_illumina_bwa_v1_1_04_acefile_updated',
            2539586 => 'soap_solexa_insert_180',
        },
        somatic_variation => {
            2594193 => 'wgs_with_sv',
            2596933 => 'wgs_snv_indel_only',
            2595664 => 'exome_with_sv',
            2624278 => 'exome_without_sv',
            2642137 => 'nov_2011_wgs',
            2642139 => 'nov_2011_exome',
        },
    );

    # LIMS - APIPE Bridge
    $self->lims_apipe_bridge;

    $self->eval_metrics(qw(
        lsf_all_apipe_builder
        lsf_all_non_apipe_builder
        lsf_all_non_apipe_builder_apipe_queues
        lsf_workflow_run
        lsf_workflow_pend
        lsf_alignment_run
        lsf_alignment_pend
        lsf_blades_run
        lsf_blades_pend

        models_build_requested
        models_build_requested_first_build
        models_buildless
        models_failed

        free_disk_space_info_genome_models
        free_disk_space_info_alignments
        free_disk_space_info_apipe_ref
        free_disk_space_prod_builder
        total_disk_space_info_genome_models
        total_disk_space_info_alignments
        total_disk_space_info_apipe_ref
        total_disk_space_prod_builder

        perl_test_duration
    ));

    # search metrics
    $self->log_metric($self->index_queue_count('where priority = 0', 'high_priority'));
    $self->log_metric($self->index_queue_count('where priority = 1', 'normal_priority'));
    $self->log_metric($self->index_queue_count('where priority not in (0, 1)', 'low_priority'));

    # Postgres DB Metrics
    $self->db_postgres_sessions;

    return 1;
}

sub lims_apipe_bridge {
    my $self = shift;

    my $timestamp = DateTime->now->strftime("%s");

    # Inprogress QIDFGM
    my $value = $self->parse_sqlrun_count(
        q{SELECT COUNT(*) FROM gsc.process_step_executions WHERE ps_ps_id = 3733 AND psesta_pse_status = 'inprogress'}, 'Genome::DataSource::Oltp'
    );
    $self->log_metric('lims_apipe_bridge.qidfgm', $value, $timestamp);

    my $select = q(SELECT COUNT(DISTINCT(d.instrument_data_id)) FROM config.instrument_data_analysis_project_bridge AS d JOIN config.analysis_project AS p ON (d.analysis_project_id = p.id));
    my @cases = (
        ['new', q(WHERE d.status = 'new' AND p.status = 'In Progress')],
        ['failed', q(WHERE d.status = 'failed' AND p.status = 'In Progress')],
        ['hold', q(WHERE d.status in ('new', 'failed') AND p.status = 'Hold')],
        ['pending', q(WHERE d.status in ('new', 'failed') AND p.status = 'Pending')],
    );
    my $in_progress_count;
    for my $case (@cases) {
        my ($name, $clause) = @$case;
        my $count = $self->parse_sqlrun_count($select . ' ' . $clause);
        $self->log_metric("lims_apipe_bridge.$name", $count, $timestamp);
        $in_progress_count += $count;
    }

    $self->log_metric('lims_apipe_bridge.inprogress', $in_progress_count, $timestamp);

    return 1;
}

sub perl_test_duration {
    my $self = shift;
    my $name = join('.', 'apipe', 'test_metrics', 'perl_tests_duration');
    my $timestamp = DateTime->now->strftime("%s");
    my $url = 'https://apipe-ci.gsc.wustl.edu/job/1-Genome-Perl-Tests/lastCompletedBuild/api/xml?xpath=matrixBuild/duration';
    my $value = qx(wget -qO - --no-check-certificate "$url" | sed -e 's/<[^>]*>//g');
    # value is originally in milliseconds, convert to minutes
    $value = $value / 1000 / 60;
    chomp($value);
    return ($name, $value, $timestamp);
}

sub build_status_by_user {
    my ($self, %params) = @_;
    my @statuses = @{$params{status}};
    my @users = @{$params{user}};

    for my $user (@users) {
        for my $status (@statuses) {
            my $name = join('.', 'builds', 'status', $user, 'current_' . lc($status));
            my $timestamp = DateTime->now->strftime("%s");

            my $user_query;
            if ($user eq 'all') {
                $user_query = " and b.run_by != 'apipe-tester'";
            }
            else {
                $user_query = " and b.run_by = '$user'";
            }

            my $value = $self->parse_sqlrun_count(
                "select count(b.build_id) builds " .
                "from model.build b " .
                "where b.status = '$status' $user_query"
            );
            $self->log_metric($name, $value, $timestamp);
        }
    }
    return 1;
}

sub model_status_by_user {
    my ($self, %params) = @_;
    my @statuses = @{$params{status}};
    my @users = @{$params{user}};

    for my $user (@users) {
        for my $status (@statuses) {
            my $name = join('.', 'models', 'status', $user, 'current_' . lc($status));
            my $timestamp = DateTime->now->strftime("%s");

            my $user_query;
            if ($user eq 'all') {
                $user_query = " and b.run_by != 'apipe-tester'";
            }
            else {
                $user_query = " and b.run_by = '$user'";
            }

            my $value = $self->parse_sqlrun_count(
                "select count(distinct m.genome_model_id) model_ids " .
                "from model.model m " .
                "where exists (" .
                    "select * from model.build b " .
                    "where b.model_id = m.genome_model_id " .
                    "and b.status = '$status' $user_query" .
                ")"
            );
            $self->log_metric($name, $value, $timestamp);
        }
    }
    return 1;
}

sub pipeline_metrics_by_processing_profile {
    my $self = shift;
    my %params = @_;
    for my $pipeline (sort keys %params) {
        my $class_name = 'Genome::Model::' . join('', map { ucfirst $_ } split('_', $pipeline)); # How's THAT for a one-liner?

        my %pp_info = %{$params{$pipeline}};
        $pp_info{'all'} = 'all';

        for my $pp_id (sort keys %pp_info) {
            my $pp_name = $pp_info{$pp_id};
            my $name = join('.', 'builds', 'running_pipelines', $pipeline, $pp_name);
            my $timestamp = DateTime->now->strftime("%s");

            my $pp_query = '';
            unless ($pp_id eq 'all') {
                $pp_query = " and m.processing_profile_id = '$pp_id'";
            }

            my $value = $self->parse_sqlrun_count(
                "select count(b.build_id) builds " .
                "from model.model m " .
                "join model.build b on b.model_id = m.genome_model_id " .
                "where b.status in ('Running', 'Scheduled', 'New') " .
                "and b.run_by != 'apipe-tester' " .
                "and m.subclass_name = '$class_name' $pp_query"
            );
            $self->log_metric($name, $value, $timestamp);
        }
    }
    return 1;
}

sub lsf_all_apipe_builder {
    my $self = shift;
    my $name = 'lsf.all.apipe-builder';
    my $timestamp = DateTime->now->strftime("%s");
    my $bjobs_output = qx(bjobs -w -u all 2> /dev/null | awk '{print \$1, \$2, \$3}' | grep "^[0-9].*apipe-builder.*RUN" | wc -l);
    my ($value) = $bjobs_output =~ /^(\d+)/;
    return ($name, $value, $timestamp);
}
sub lsf_all_non_apipe_builder {
    my $self = shift;
    my $name = 'lsf.all.non-apipe-builder';
    my $timestamp = DateTime->now->strftime("%s");
    my $bjobs_output = qx(bjobs -w -u all 2> /dev/null | awk '{print \$1, \$2, \$3, \$4}' | egrep -v "\(apipe-builder|workflow\)"| grep "^[0-9].*RUN" | wc -l);
    my ($value) = $bjobs_output =~ /^(\d+)/;
    return ($name, $value, $timestamp);
}
sub lsf_all_non_apipe_builder_apipe_queues {
    my $self = shift;
    my $name = 'lsf.all.non-apipe-builder-apipe-queues';
    my $timestamp = DateTime->now->strftime("%s");
    my $bjobs_output = qx(bjobs -w -u all 2> /dev/null | awk '{print \$1, \$2, \$3, \$4}' | grep -v apipe-builder | egrep "^[0-9].*RUN.*\(apipe|alignment\)" | wc -l);
    my ($value) = $bjobs_output =~ /^(\d+)/;
    return ($name, $value, $timestamp);
}
sub lsf_queue_status {
    my $self = shift;
    my ($queue, $status) = @_;
    my $name = join('.', 'lsf', $queue, lc($status));
    my $timestamp = DateTime->now->strftime("%s");
    my $bjobs_output = qx(bjobs -u apipe-builder -q $queue 2> /dev/null | grep ^[0-9] | grep $status | wc -l);
    my ($value) = $bjobs_output =~ /^(\d+)/;
    return ($name, $value, $timestamp);
}
sub lsf_workflow_run {
    my $self = shift;
    return $self->lsf_queue_status('workflow', 'RUN');
}
sub lsf_workflow_pend {
    my $self = shift;
    return $self->lsf_queue_status('workflow', 'PEND');
}
sub lsf_alignment_run {
    my $self = shift;
    return $self->lsf_queue_status('alignment-pd', 'RUN');
}
sub lsf_alignment_pend {
    my $self = shift;
    return $self->lsf_queue_status('alignment-pd', 'PEND');
}
sub lsf_blades_status {
    my $self = shift;
    my $status = shift;
    my ($long_name, $long_value, $long_timestamp) = $self->lsf_queue_status('long', $status);
    my ($apipe_name, $apipe_value, $apipe_timestamp) = $self->lsf_queue_status('apipe', $status);
    (my $name = $long_name) =~ s/long/blades/g;
    my $timestamp = $long_timestamp;
    my $value = $long_value + $apipe_value;
    return ($name, $value, $timestamp);
}
sub lsf_blades_run {
    my $self = shift;
    return $self->lsf_blades_status('RUN');
}
sub lsf_blades_pend {
    my $self = shift;
    return $self->lsf_blades_status('PEND');
}

sub models_build_requested {
    my $self = shift;
    my $name = join('.', 'models', 'build_requested');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count("select count(*) from model.model gm where gm.build_requested = '1'");
    return ($name, $value, $timestamp);
}
sub models_build_requested_first_build {
    my $self = shift;
    my $name = join('.', 'models', 'build_requested_first_build');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count("select count(*) from model.model gm where gm.build_requested = '1' and not exists (select * from model.build gmb where gmb.model_id = gm.genome_model_id)");
    return ($name, $value, $timestamp);
}
sub models_buildless {
    my $self = shift;
    my $name = join('.', 'models', 'buildless');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count("select count(*) from model.model gm where gm.build_requested != '1' and gm.run_as in ('apipe-builder', 'prod-builder') and not exists (select * from model.build gmb where gmb.model_id = gm.genome_model_id)");
    return ($name, $value, $timestamp);
}
sub models_failed {
    my $self = shift;
    my $name = join('.', 'models', 'failed');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count("select count(distinct(gm.genome_model_id)) from model.model gm where exists (select * from model.build gmb where gmb.model_id = gm.genome_model_id and gmb.status = 'Failed' and gm.run_as in ('apipe-builder', 'prod-builder'))");
    return ($name, $value, $timestamp);
}

sub get_free_space_for_disk_group {
    my $self = shift;
    my $group = shift;
    my $value = $self->parse_sqlrun_count(
        "select cast((sum(greatest(v.unallocated_kb - ceil(least((total_kb * .05), 1073741824)), 0)) / 1073741824) as number(10,4)) free_space " .
        "from gsc.disk_volume v " .
        "join gsc.disk_volume_group dvg on dvg.dv_id = v.dv_id " .
        "join gsc.disk_group g on g.dg_id = dvg.dg_id " .
        "where g.disk_group_name = '$group' " .
        "and v.can_allocate = 1 " .
        "and v.disk_status = 'active'"

        ,'Genome::DataSource::Oltp'
    );
    return $value;
}

sub free_disk_space_info_genome_models {
    my $self = shift;
    my $name = join('.', 'disk', 'available', 'info_genome_models');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_free_space_for_disk_group('info_genome_models');
    return ($name, $value, $timestamp);
}

sub free_disk_space_prod_builder {
    my $self = shift;
    my $name = join('.', 'disk', 'available', 'prod-builder');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_free_space_for_disk_group('prod-builder');
    return ($name, $value, $timestamp);
}

sub free_disk_space_info_alignments {
    my $self = shift;
    my $name = join('.', 'disk', 'available', 'info_alignments');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_free_space_for_disk_group('info_alignments');
    return ($name, $value, $timestamp);
}

sub free_disk_space_info_apipe_ref {
    my $self = shift;
    my $name = join('.', 'disk', 'available', 'info_apipe_ref');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_free_space_for_disk_group('info_apipe_ref');
    return ($name, $value, $timestamp);
}

sub get_total_space_for_disk_group {
    my $self = shift;
    my $group = shift;
    my $value = $self->parse_sqlrun_count(
        "select cast((sum(greatest(total_kb - least((total_kb * .05), 1073741824), 0)) / 1073741824) as number(10,4)) total_space " .
        "from gsc.disk_volume v " .
        "join gsc.disk_volume_group dvg on dvg.dv_id = v.dv_id " .
        "join gsc.disk_group g on g.dg_id = dvg.dg_id " .
        "where g.disk_group_name = '$group' " .
        "and v.can_allocate = 1 " .
        "and v.disk_status = 'active'"

        ,'Genome::DataSource::Oltp'
    );
    return $value;
}

sub total_disk_space_info_genome_models {
    my $self = shift;
    my $name = join('.', 'disk', 'total', 'info_genome_models');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_total_space_for_disk_group('info_genome_models');
    return ($name, $value, $timestamp);
}

sub total_disk_space_prod_builder {
    my $self = shift;
    my $name = join('.', 'disk', 'total', 'prod-builder');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_total_space_for_disk_group('prod-builder');
    return ($name, $value, $timestamp);
}

sub total_disk_space_info_alignments {
    my $self = shift;
    my $name = join('.', 'disk', 'total', 'info_alignments');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_total_space_for_disk_group('info_alignments');
    return ($name, $value, $timestamp);
}

sub total_disk_space_info_apipe_ref {
    my $self = shift;
    my $name = join('.', 'disk', 'total', 'info_apipe_ref');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->get_total_space_for_disk_group('info_apipe_ref');
    return ($name, $value, $timestamp);
}

sub allocations_needing_reallocating {
    my $self = shift;
    my $name = join('.', 'disk', 'allocation', 'not_reallocated');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count(
        "select count(*) " .
        "from disk.allocation a " .
        "where a.creation_time < current_date - 7 " .
        "and a.reallocation_time is null"
    );
    return ($name, $value, $timestamp);
}

sub index_queue_count {
    my ($self, $sql_suffix, $name_suffix) = @_;
    my $name = join('.', 'search', 'index_queue_count');
    my $timestamp = DateTime->now->strftime("%s");
    my $sql = 'select count(*) from web.search_index_queue';
    if ($sql_suffix && $name_suffix) {
        $name .= ".$name_suffix";
        $sql .= " $sql_suffix";
    }
    my $value = $self->parse_sqlrun_count($sql, 'Genome::DataSource::GMSchema');
    return ($name, $value, $timestamp);
}

sub db_postgres_sessions {
    my $self = shift;
    my $name = join('.','db','postgres','sessions');
    my $timestamp = DateTime->now->strftime("%s");
    my $value = $self->parse_sqlrun_count(q{select count(*) from pg_stat_activity}, 'Genome::DataSource::GMSchema');
    $self->log_metric($name, $value, $timestamp);
}
