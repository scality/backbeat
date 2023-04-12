from grafanalib.core import (
    ConstantInput,
    DataSourceInput,
    Heatmap,
    HeatmapColor,
    RowPanel,
    Template,
    Templating,
    Threshold,
    YAxis,
)

from grafanalib import formatunits as UNITS
from scalgrafanalib import (
    layout,
    metrics,
    Dashboard,
    GaugePanel,
    PieChart,
    Stat,
    Target,
    TimeSeries,
    Tooltip,
)


import os, sys
sys.path.append(os.path.abspath(f'{__file__}/../../..'))
from monitoring import s3_circuit_breaker


class Metrics:
    QUEUE_LENGTH, QUEUE_SIZE = [
        metrics.CounterMetric(
            name, namespace="${namespace}", job="${job_queue_populator}"
        )
        for name in [
            's3_replication_populator_objects_total',
            's3_replication_populator_bytes_total'
        ]
    ]

    READ, WRITE, DATA_BYTES, META_BYTES, SRC_BYTES, DATA_STATUS, META_STATUS = [
        metrics.CounterMetric(
            name,
            'location', 'job', *extraLabels, namespace='${namespace}'
        ).with_defaults(
            'job=~"$queue_processor"', 'location=~"$location"',
        )
        for name, extraLabels in {
            's3_replication_data_read_total': [],
            's3_replication_data_write_total': [],
            's3_replication_data_bytes_total': [],
            's3_replication_metadata_bytes_total': [],
            's3_replication_source_data_bytes_total': [],
            's3_replication_data_status_changed_total': ['replicationStatus'],
            's3_replication_metadata_status_changed_total': ['replicationStatus'],
        }.items()
    ]

    STAGE_DURATION = metrics.BucketMetric(
        's3_replication_stage_time_elapsed',
        'location', 'job', 'replicationStage', namespace='${namespace}'
    ).with_defaults(
        'job=~"$queue_processor"', 'location=~"$location"',
    )

    RPO = metrics.BucketMetric(
        's3_replication_rpo_seconds',
        namespace="${namespace}", job="${job_data_processor}",
    ).with_description(
        'RPO is defined as the difference between the time an object was '
        'written to and the time when it is picked for replication by the '
        'data-processor'
    )

    LATENCY = metrics.BucketMetric(
        's3_replication_latency_seconds',
        namespace="${namespace}", job="${job_status_processor}",
    ).with_description(
        'Replication Latency is defined as time taken for an object to '
        'replicate successfully to the destination'
    )

    STATUS = metrics.BucketMetric(
        's3_replication_status_process_duration_seconds',
        'result', 'replicationStatus',
        namespace='${namespace}', job="${job_status_processor}"
    )

    STATUS_CHANGED = metrics.CounterMetric(
        's3_replication_status_changed_total', 'replicationStatus',
        namespace='${namespace}', job="${job_status_processor}"
    )

    REPLAY_OBJECTS_COMPLETED = metrics.CounterMetric(
        's3_replication_replay_objects_completed_total',
        'location', 'replayCount', 'replicationStatus',
        namespace='${namespace}', job="${job_status_processor}"
    ).with_defaults(
        'location=~"$location"',
    )

    REPLAY_SUCCESS, REPLAY_ATTEMPTS = [
        metrics.CounterMetric(
            m, 'location', 'replayCount',
            namespace='${namespace}', job="${job_status_processor}"
        ).with_defaults(
            'location=~"$location"',
        )
        for m in ['s3_replication_replay_success_total', 's3_replication_replay_attempts_total']
    ]

    REPLAY_FILE_SIZE = metrics.BucketMetric(
        's3_replication_replay_file_sizes_completed', 'location', 'replayCount', 'replicationStatus',
        namespace='${namespace}', job="${job_status_processor}"
    ).with_defaults(
        'location=~"$location"',
    )

    REPLAY_COUNT = metrics.BucketMetric(
        's3_replication_replay_count', 'location', 'replicationStatus',
        namespace='${namespace}', job="${job_status_processor}"
    ).with_defaults(
        'location=~"$location"',
    )


JOBS={
    'queue_populator':  'backbeat-replication-producer-headless',
    'data_processor':   'backbeat-replication-data-processor-headless',
    'replay_processor': 'backbeat-replication-replay-processor-headless',
    'status_processor': 'backbeat-replication-status-processor-headless',
    'zookeeper':        'base-quorum-headless',
    'kafka':            'base-queue',
}
DEFAULT_JOB_PREFIX='artesca-data-'

TOPICS={
    'replication_topic': 'backbeat-replication',
    'status_topic':      'backbeat-replication-status',
    'replay_topic':      'backbeat-replication-replay-0',
    'failed_topic':      'backbeat-replication-failed',
}
INSTANCE_ID_RE='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\\\.'

DEFAULT_GROUP_PREFIX='backbeat-replication-group-'


def up(component: str, expr: str = None, title: str = None, **kwargs):
    return Stat(
        title=title or component.replace('_', ' ').title(),
        dataSource='${DS_PROMETHEUS}',
        minValue='0',
        maxValue='${' + component + '_replicas}',
        reduceCalc='last',
        targets=[Target(
            expr=expr or '\n'.join([
                'sum(up{',
                '    namespace="${namespace}",',
                '    job="${job_' + component + '}"',
                '})',
            ]),
        )],
        thresholdType='percentage',
        thresholds=[
            Threshold('red', 0, 0.0),
            Threshold('yellow', 1, 50.),
            Threshold('green', 2, 100.),
        ],
        **kwargs)


zookeeper_quorum = up(
    title='Zookeeper Quorum',
    component='zookeeper',
    expr='\n'.join([
        'max(quorum_size{',
        '    namespace="${namespace}",',
        '    job="${job_zookeeper}"',
        '})'
    ]),
)

kafka_brokers = up(
    title='Kafka brokers',
    component='kafka',
    expr='\n'.join([
        'count(kafka_server_replicamanager_leadercount{',
        '    namespace="${namespace}",',
        '    job="${job_kafka}"',
        '})',
    ]),
)

kafka_offline = Stat(
    title='Offline partitions',
    dataSource='${DS_PROMETHEUS}',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'sum(kafka_controller_kafkacontroller_offlinepartitionscount{',
            '    namespace="${namespace}",',
            '    job="${job_kafka}",',
            '})',
        ]),
    )],
    thresholds=[
        Threshold('green', 0, 0.),
        Threshold('red', 1, 1.),
    ],
)

kafka_underreplicated = Stat(
    title='Underreplicated partitions',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            '    sum(kafka_cluster_partition_underreplicated{',
            '        namespace="${namespace}",',
            '        job="${job_kafka}",',
            '        topic=~"^(' + '|'.join(['${' + t + '}' for t in TOPICS]) + ')$",',
            '    }) by(topic),',
            '"topic", "$1", "topic", "' + INSTANCE_ID_RE + '(?:backbeat-)?(.*)$")',
        ]),
        legendFormat='{{topic}}',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
        Threshold('red', 1, 1.),
    ],
)


replication_object_rate = Stat(
    title='Replication Rate',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    decimals=1,
    format=UNITS.OPS_PER_SEC,
    reduceCalc='mean',
    targets=[Target(
        expr='sum(rate(' + Metrics.WRITE() + ')) or vector(0)',
    )],
    thresholds=[Threshold('semi-dark-blue', 0, 0.)],
)

replication_rate_by_location = TimeSeries(
    title='Replication Rate by location',
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    fillOpacity=30,
    unit=UNITS.OPS_PER_SEC,
    lineInterpolation='smooth',
    targets=[Target(
        expr='sum(rate(' + Metrics.WRITE() + ')) by(location)',
        legendFormat="{{location}}",
    )],
)

replication_data_rate = Stat(
    title='Replication Speed',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    decimals=1,
    format=UNITS.BYTES_SEC,
    reduceCalc='mean',
    targets=[Target(
        expr='sum(rate(' + Metrics.DATA_BYTES() + ')) or vector(0)',
    )],
    thresholds=[Threshold('semi-dark-blue', 0, 0.)],
)

replication_data_rate_by_location = TimeSeries(
    title='Replication Speed by location',
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    fillOpacity=30,
    unit=UNITS.BYTES_SEC,
    lineInterpolation='smooth',
    targets=[Target(
        expr='sum(rate(' + Metrics.DATA_BYTES() + ')) by(location)',
        legendFormat="{{location}}",
    )],
)

success_rate = GaugePanel(
    title='Success rate',
    dataSource='${DS_PROMETHEUS}',
    calc='mean',
    decimals=2,
    format=UNITS.PERCENT_FORMAT,
    min=0,
    max=100,
    noValue='-',
    targets=[Target(
        expr='\n'.join([
            '100 / (1 + sum(rate(' + Metrics.STATUS_CHANGED(replicationStatus='FAILED') + '))',
            '           /',
            '           sum(rate(' + Metrics.STATUS_CHANGED(replicationStatus='COMPLETED') + '))',
            '      )',
            'or vector(100)',
        ]),
    )],
    thresholds=[
        Threshold('#808080', 0, 0.0),
        Threshold('red',     1, 0.0),
        Threshold('orange',  2, 95.0),
        Threshold('yellow',  3, 99.0),
        Threshold('green',   4, 100.0),
    ],
)

replay_rate = Stat(
    title='Replays',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    format=UNITS.OPS_PER_SEC,
    reduceCalc='mean',
    targets=[Target(
        expr='sum(rate(' + Metrics.REPLAY_ATTEMPTS() + ')) or vector(0)',
    )],
    thresholds=[Threshold('dark-purple', 0, 0.)],
)

error_rate = Stat(
    title='Errors',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    format=UNITS.OPS_PER_SEC,
    reduceCalc='mean',
    targets=[Target(
        expr='\n'.join([
            'sum(rate(',
            '  ' + Metrics.STATUS_CHANGED(replicationStatus='FAILED'),
            ')) or vector(0)',
        ])
    )],
    thresholds=[Threshold('dark-purple', 0, 0.)],
)

queue_populator_replication_backlog = TimeSeries(
    title='Replication backlog size',
    dataSource='${DS_PROMETHEUS}',
    description='Number of entries which are yet to be processed by replication',
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.SHORT,
    targets=[Target(
        expr='\n'.join([
            'max(kafka_consumergroup_group_lag{',
            '    namespace="${namespace}",',
            '    cluster_name="${job_kafka}",',
            '    topic=~"${replication_topic}"',
            '})',
        ]),
    )],
    colorMode="thresholds",
    thresholds=[Threshold('dark-blue', 0, 0.)],
)

replication_rpo_avg = Stat(
    title='Avg RPO',
    description=Metrics.RPO.description,
    dataSource='${DS_PROMETHEUS}',
    decimals=1,
    format=UNITS.SECONDS,
    noValue='-',
    reduceCalc='lastNotNull',
    targets=[Target(
        expr='\n'.join([
            'sum(rate(' + Metrics.RPO.sum() + '))',
            '  /',
            'sum(rate(' + Metrics.RPO.count() + '))',
        ])
    )],
    thresholds=[
        Threshold("#808080", 0, 0.0),
        Threshold("blue", 1, 0.0),
    ],
)

replication_rpo = TimeSeries(
    title='Replication RPO - 99 percentile',
    description=Metrics.RPO.description,
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    fillOpacity=30,
    unit=UNITS.SECONDS,
    lineInterpolation='smooth',
    targets=[Target(
        expr='\n'.join([
            'histogram_quantile(0.99,',
            '   sum(rate(' + Metrics.RPO.bucket() + '))',
            '   by(le, location))',
        ]),
        legendFormat="{{location}}",
    )],
)

replication_latency_avg = Stat(
    title='Avg latency',
    description=Metrics.LATENCY.description,
    dataSource='${DS_PROMETHEUS}',
    decimals=1,
    format=UNITS.SECONDS,
    noValue='-',
    reduceCalc='lastNotNull',
    targets=[Target(
        expr='\n'.join([
            'sum(rate(' + Metrics.LATENCY.sum() + '))',
            '  /',
            'sum(rate(' + Metrics.LATENCY.count() + '))',
        ])
    )],
    thresholds=[
        Threshold("#808080", 0, 0.0),
        Threshold("blue", 1, 0.0),
    ],
)

replication_latency = TimeSeries(
    title='Replication Latency - 99 percentile',
    description=Metrics.LATENCY.description,
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    fillOpacity=30,
    unit=UNITS.SECONDS,
    lineInterpolation='smooth',
    targets=[Target(
        expr='\n'.join([
            'histogram_quantile(0.99,',
            '   sum(rate(' + Metrics.LATENCY.bucket() + '))',
            '   by(le, location)',
            ')'
        ]),
        legendFormat="{{location}}",
    )],
)

queue_populator_lag = TimeSeries(
    title='Replication populator lag',
    description=(
        'Delay between the time an object is updated and the time it'
        'is picked up by Queue Populator.'
        ''
        'The precision is affected however by Prometheus polling interval,'
        'and delays up to this interval are expected.'
    ),
    dataSource='${DS_PROMETHEUS}',
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='\n'.join([
            'clamp_min(',
            '   max(mongodb_mongod_replset_oplog_head_timestamp{',
            '           namespace="${namespace}", job="${job_mongod}"})',
            '   -',
            '   min(s3_replication_log_timestamp{namespace="${namespace}",',
            '             job="${job_queue_populator}"}),',
            '0)',
        ]),
    )],
)

queue_populator_kafka_injection_rate = TimeSeries(
    title='Kafka injection rate',
    dataSource='${DS_PROMETHEUS}',
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='\n'.join([
            'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{',
            '            namespace="${namespace}",',
            '            job="${job_kafka}",',
            '            topic=~"${replication_topic}"',
            '         }[$__rate_interval])',
            ')',
        ]),
    )],
)

queue_populator_objects_count = Stat(
    title='Queued objects count',
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    format=UNITS.SHORT,
    reduceCalc='last',
    targets=[Target(
        expr='sum(increase(' + Metrics.QUEUE_LENGTH() + '))',
    )],
    thresholds=[Threshold('semi-dark-blue', 0, 0.)],
)

queue_populator_objects_size = Stat(
    title='Queued objects size',
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    format=UNITS.BYTES,
    reduceCalc='last',
    targets=[Target(
        expr='sum(increase(' + Metrics.QUEUE_SIZE() + '))',
    )],
    thresholds=[Threshold('semi-dark-blue', 0, 0.)],
)

queue_populator_objects_rate = TimeSeries(
    title='Queued objects count/s',
    dataSource='${DS_PROMETHEUS}',
    decimals=1,
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.QUEUE_LENGTH() + '))',
    )],
)

queue_populator_objects_datarate = TimeSeries(
    title='Queued object size/s',
    dataSource='${DS_PROMETHEUS}',
    decimals=1,
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.BYTES_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.QUEUE_SIZE() + '))',
    )],
)

queue_processor_lag = [
    TimeSeries(
        title=name + ' processor lag',
        dataSource='${DS_PROMETHEUS}',
        fillOpacity=30,
        lineInterpolation='smooth',
        unit=UNITS.SHORT,
        targets=[Target(
            expr='\n'.join([
                'label_replace(',
                '    max(kafka_consumergroup_group_lag{',
                '        namespace="${namespace}",',
                '        cluster_name="${job_kafka}",',
                '        topic=~"' + topic + '",',
                '        group=~"${queue_processor_group}($location)$",',
                '    }) by(group),',
                '"group", "$1", "group", "${queue_processor_group}(.*)$")',
            ]),
            legendFormat='{{group}}'
        )],
    )
    for name, topic in {
        'Data': '${replication_topic}',
        'Replay': '${replay_topic}',
    }.items()
]

queue_processor_rate = [
    TimeSeries(
        title='Replication ' + name + ' rate by location',
        dataSource='${DS_PROMETHEUS}',
        decimals=1,
        fillOpacity=30,
        lineInterpolation='smooth',
        unit=UNITS.OPS_PER_SEC,
        targets=[Target(
            expr='sum(rate(' + metric + ')) by(location)',
            legendFormat='{{location}}'
        )],
    )
    for name, metric in {
        'read': Metrics.READ(),
        'write': Metrics.WRITE(),
    }.items()
]

queue_processor_speed = [
    TimeSeries(
        title='Replication ' + name + ' speed by location',
        dataSource='${DS_PROMETHEUS}',
        decimals=1,
        fillOpacity=30,
        lineInterpolation='smooth',
        unit=UNITS.BYTES_SEC,
        targets=[Target(
            expr='sum(rate(' + metric + ')) by(location)',
            legendFormat='{{location}}'
        )],
    )
    for name, metric in {
        'read': Metrics.SRC_BYTES(),
        'write': Metrics.DATA_BYTES(),
        'metadata write': Metrics.META_BYTES()
    }.items()
]

queue_processor_status_completed, queue_processor_status_failed = [
    TimeSeries(
        title='Replication ' + name + ' by location',
        dataSource='${DS_PROMETHEUS}',
        decimals=1,
        fillOpacity=30,
        lineInterpolation='smooth',
        unit=UNITS.OPS_PER_SEC,
        targets=[Target(
            expr='sum(rate(' + metric + ')) by(location)',
            legendFormat='{{location}}'
        )],
    )
    for name, metric in {
        'completed': Metrics.META_STATUS(replicationStatus='COMPLETED'),
        'failed': Metrics.META_STATUS(replicationStatus='FAILED'),
    }.items()
]

queue_processor_ops_by_location, queue_processor_errors_by_location = [
    PieChart(
        title='Distribution ' + name,
        dataSource='${DS_PROMETHEUS}',
        legendDisplayMode='hidden',
        pieType='donut',
        reduceOptionsCalcs=['mean'],
        unit=UNITS.OPS_PER_SEC,
        targets=[Target(
            expr='\n'.join([
                'sum(rate(' + metric + ')) by(location)',
            ]),
            legendFormat='{{location}}'
        )],
    )
    for name, metric in {
        'by location': Metrics.META_STATUS(),
        'of errors': Metrics.META_STATUS(replicationStatus='FAILED'),
    }.items()
]

queue_processor_stage_time = [
    Heatmap(
        title='Time in ' + stageTitle + ' stage',
        dataSource='${DS_PROMETHEUS}',
        dataFormat='tsbuckets',
        hideZeroBuckets=True,
        maxDataPoints=15,
        tooltip=Tooltip(show=True, showHistogram=True),
        yAxis=YAxis(format=UNITS.MILLI_SECONDS, decimals=0),
        cards={'cardPadding': 1, 'cardRound': 2},
        color=HeatmapColor(mode='opacity'),
        targets=[Target(
            expr='\n'.join([
                'sum(increase(',
                '  ' + Metrics.STAGE_DURATION.bucket(replicationStage=stageName),
                ')) by(le)'
            ]),
            format="heatmap",
            legendFormat="{{le}}",
        )],
    )
    for stageTitle, stageName in {
        'read': 'ReplicationSourceDataRead',
        'data write': 'ReplicationDestinationDataWrite',
        'metadata write': 'ReplicationDestinationMetadataWrite',
    }.items()
]

queue_processor_stage_avg = [
    Stat(
        title='Avg ' + stageTitle + ' stage duration',
        description=Metrics.LATENCY.description,
        dataSource='${DS_PROMETHEUS}',
        decimals=1,
        format=UNITS.MILLI_SECONDS,
        noValue='-',
        reduceCalc='lastNotNull',
        targets=[Target(
            expr='\n'.join([
                'sum(rate(' + Metrics.STAGE_DURATION.sum(replicationStage=stageName) + '))',
                '  /',
                'sum(rate(' + Metrics.STAGE_DURATION.count(replicationStage=stageName) + '))',
            ])
        )],
        thresholds=[
            Threshold("#808080", 0, 0.0),
            Threshold("blue", 1, 0.0),
        ],
    )
    for stageTitle, stageName in {
        'read': 'ReplicationSourceDataRead',
        'data write': 'ReplicationDestinationDataWrite',
        'metadata write': 'ReplicationDestinationMetadataWrite',
    }.items()
]

queue_processor_circuit_breaker = s3_circuit_breaker(
    'Flow Control',
    process='replication_queue_processor',
    job='${job_data_processor}',
)


replay_processor_rate, replay_processor_attempts, replay_processor_success_attempts, replay_processor_failed_attempts = [
    TimeSeries(
        title='Replay ' + name + ' by location',
        dataSource='${DS_PROMETHEUS}',
        decimals=1,
        fillOpacity=30,
        lineInterpolation='smooth',
        unit=UNITS.OPS_PER_SEC,
        targets=[Target(
            expr='sum(rate(' + metric + ')) by(location)',
            legendFormat='{{location}}'
        )],
    )
    for name, metric in {
        'rate' : Metrics.REPLAY_ATTEMPTS(replayCount=1),
        'attempts' : Metrics.REPLAY_ATTEMPTS(),
        'success' : Metrics.REPLAY_SUCCESS(),
        'failed' : Metrics.REPLAY_OBJECTS_COMPLETED(
            'replayCount!="0"', replicationStatus="FAILED"
        )
    }.items()
]

replay_count = Heatmap(
    title='Replay count distribution',
    dataSource='${DS_PROMETHEUS}',
    dataFormat='tsbuckets',
    hideZeroBuckets=True,
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SHORT, decimals=0),
    yBucketBound='middle',
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='\n'.join([
            'label_replace('
            '  sum(increase(',
            '    ' + Metrics.REPLAY_COUNT.bucket(),
            '  )) by(le),',
            '"le", "Failed", "le", "\\\\+Inf")'
        ]),
        format="heatmap",
        legendFormat="{{le}}",
    )],
)

replay_by_size = Heatmap(
    title='Replay distribution by size',
    dataSource='${DS_PROMETHEUS}',
    dataFormat='tsbuckets',
    hideZeroBuckets=True,
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.BYTES, decimals=0),
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='\n'.join([
            'sum(increase(',
            '  ' + Metrics.REPLAY_FILE_SIZE.bucket('replayCount!="0"'),
            ')) by(le)',
        ]),
        format="heatmap",
        legendFormat="{{le}}",
    )],
)

status_processor_partition_lag = TimeSeries(
    title='Status processor lag',
    dataSource='${DS_PROMETHEUS}',
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.SHORT,
    targets=[Target(
        expr='\n'.join([
            'max(kafka_consumergroup_group_lag{',
            '    namespace="${namespace}",',
            '    cluster_name="${job_kafka}",',
            '    topic=~"${status_topic}"',
            '})',
        ]),
    )],
)

status_processor_rate = TimeSeries(
    title='Status processor ops rate',
    dataSource='${DS_PROMETHEUS}',
    fillOpacity=30,
    legendDisplayMode='hidden',
    lineInterpolation='smooth',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.STATUS.count() + ')) or vector(0)',
    )],
)

status_processor_avg_latency = TimeSeries(
    title='Average latency',
    dataSource='${DS_PROMETHEUS}',
    fillOpacity=30,
    lineInterpolation='smooth',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='\n'.join([
            'sum(rate(' + Metrics.STATUS.sum() + ')) by(result)',
            '  /',
            'sum(rate(' + Metrics.STATUS.count() + ')) by(result)',
        ]),
        legendFormat="{{result}}",
    )],
)

dashboard = (
    Dashboard(
        title='Replication',
        editable=True,
        refresh='30s',
        tags=['Backeat'],
        timezone='',
        inputs=[
            DataSourceInput(
                name='DS_PROMETHEUS',
                label='Prometheus',
                pluginId='prometheus',
                pluginName='Prometheus',
            ),
            ConstantInput(
                name='namespace',
                label='namespace',
                description='Namespace associated with the Zenko instance',
                value='zenko',
            ),
            ConstantInput(
                name='job_mongod',
                label='Mongod',
                description='Name of the mongod (shard) job to filter metrics',
                value='zenko/data-db-mongodb-sharded-shard0-data',
            ),
            *[
                ConstantInput(
                    name='job_' + name,
                    label=name.replace('_', ' ').title(),
                    description='Name of the ' + name + ' job to filter metrics',
                    value='artesca-data-' + value,
                ) for name, value in JOBS.items()
            ],
            *[
                ConstantInput(
                    name=name + '_replicas',
                    label=name.replace('_', ' ').title() + ' replicas',
                    description='Expected number of replicas for ' + name,
                    value=1,
                ) for name in JOBS
            ],
            *[
                ConstantInput(
                    name=name,
                    label=name.replace('_', ' ').title(),
                    description='Name of the ' + name + 'topic',
                    value=INSTANCE_ID_RE + value,
                ) for name, value in TOPICS.items()
            ],
            ConstantInput(
                name='queue_processor_group',
                label='Queue Processor Group',
                description='Name of the prefix used for the queue processor groups',
                value=INSTANCE_ID_RE + DEFAULT_GROUP_PREFIX,
            )
        ],
        templating=Templating([
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Location',
                name='location',
                query='label_values(' + Metrics.RPO.bucket.raw() + ', location)',
                allValue='.*',
                includeAll=True,
                multi=True,
            ),
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Queue processor',
                name='queue_processor',
                query='label_values(' + Metrics.STAGE_DURATION.bucket.raw(
                    'job=~"${job_data_processor}|${job_replay_processor}"'
                ) + ', job)',
                regex='/^(?<value>(' + DEFAULT_JOB_PREFIX + ')?(backbeat-replication-)?(?<text>.*?)(-headless)?)$/',
                allValue='.*',
                includeAll=True,
                multi=True,
            )
        ]),
        panels=layout.column([
            RowPanel(title="Overview"),
            layout.row([
                up(component)
                for component, value in JOBS.items() if value.startswith('backbeat')
            ] + [zookeeper_quorum, kafka_brokers, kafka_offline, kafka_underreplicated], height=4),
            layout.row([
                replication_object_rate, replication_data_rate,
                success_rate, replay_rate, error_rate,
                *layout.resize([queue_populator_replication_backlog], width=6),
                layout.column([replication_rpo_avg, replication_latency_avg],
                              width=3, height=2),
            ], height=4),
            layout.row([
                replication_rate_by_location, replication_data_rate_by_location
            ], height=6),
            layout.row([
                replication_rpo, replication_latency,
            ], height=6),
            RowPanel(title="Queue Populator", collapsed=True, panels=layout.column([
                layout.row([
                    queue_populator_lag, queue_populator_kafka_injection_rate,
                    layout.column([queue_populator_objects_count, queue_populator_objects_size],
                                  width=4, height=3),
                ], height=6),
                layout.row([
                    queue_populator_objects_rate, queue_populator_objects_datarate,
                ], height=6),
            ])),
            RowPanel(title="Queue Processor", collapsed=True, panels=layout.column([
                layout.row(queue_processor_lag, height=6),
                layout.row(queue_processor_rate, height=6),
                layout.row(queue_processor_speed, height=6),
                layout.row([
                    queue_processor_status_completed,
                    *layout.resize([queue_processor_ops_by_location], width=4),
                    queue_processor_status_failed,
                    *layout.resize([queue_processor_errors_by_location],  width=4),
                ], height=6),
                layout.row([
                    *queue_processor_stage_time,
                    layout.column(queue_processor_stage_avg, width=3, height=3)
                ], height=9),
                layout.row([queue_processor_circuit_breaker], height=10),
            ])),
            RowPanel(title="Replay", collapsed=True, panels=layout.column([
                layout.row([replay_processor_rate, replay_processor_attempts],
                           height=6),
                layout.row([replay_processor_success_attempts, replay_processor_failed_attempts],
                           height=6),
                layout.row([replay_count, replay_by_size], height=6),
            ])),
            RowPanel(title="Status Processor", collapsed=True, panels=layout.column([
                layout.row([
                    status_processor_partition_lag, status_processor_rate, status_processor_avg_latency,
                ], height=6),
            ])),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
