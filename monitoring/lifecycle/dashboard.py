import os
import sys

from grafanalib.core import (
    ConstantInput,
    DataSourceInput,
    Heatmap,
    HeatmapColor,
    RowPanel,
    REFRESH_ON_TIME_RANGE_CHANGE,
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

sys.path.append(os.path.abspath(f'{__file__}/../../..'))
from monitoring import s3_circuit_breaker, s3_circuit_breaker_over_time # noqa: E402, pylint: disable=C0413


STATUS_CODE_2XX = '2..'
STATUS_CODE_3XX = '3..'
STATUS_CODE_4XX = '4..'
STATUS_CODE_5XX = '5..'

ALL_JOBS=[
    '${job_lifecycle_producer}',
    '${job_lifecycle_bucket_processor}',
    '${job_lifecycle_object_processor}',
    '${job_lifecycle_transition_processor}',
    '${job_lifecycle_gc_processor}',
    '${job_lifecycle_populator}',
    '${job_sorbet_forwarder}.*',
]

class Metrics:
    LATEST_BATCH_START_TIME = metrics.Metric(
        's3_lifecycle_latest_batch_start_time',
        job='${job_lifecycle_producer}', namespace='${namespace}',
    )

    BUCKET_LISTING_SUCCESS, BUCKET_LISTING_ERROR, BUCKET_LISTING_THROTTLING = [
        metrics.CounterMetric(
            name, job='${job_lifecycle_producer}', namespace='${namespace}',
        )
        for name in [
            's3_lifecycle_conductor_bucket_list_success_total',
            's3_lifecycle_conductor_bucket_list_error_total',
            's3_lifecycle_conductor_bucket_list_throttling_total',
        ]
    ]

    ACTIVE_INDEXING_JOBS = metrics.Metric(
        's3_lifecycle_active_indexing_jobs',
        job='${job_lifecycle_producer}', namespace='${namespace}',
    )

    LEGACY_TASKS = metrics.CounterMetric(
        's3_lifecycle_legacy_tasks_total',
        'status', job='${job_lifecycle_producer}', namespace='${namespace}',
    )

    S3_OPS = metrics.CounterMetric(
       's3_lifecycle_s3_operations_total',
       'origin', 'op', 'status', job=['$jobs'], namespace='${namespace}',
    )

    TRIGGER_LATENCY, LATENCY, DURATION = [
        metrics.BucketMetric(
            name, 'type', location=['$locations'], job=['$jobs'], namespace='${namespace}',
        )
        for name in [
            's3_lifecycle_trigger_latency_seconds',
            's3_lifecycle_latency_seconds',
            's3_lifecycle_duration_seconds',
        ]
    ]

    KAFKA_PUBLISH_SUCCESS, KAFKA_PUBLISH_ERROR = [
        metrics.CounterMetric(
            name, 'origin', 'op', namespace='${namespace}', job=['$jobs'],
        )
        for name in [
            's3_lifecycle_kafka_publish_success_total',
            's3_lifecycle_kafka_publish_error_total',
        ]
    ]


class GcMetrics:
    S3_OPS = metrics.CounterMetric(
       's3_gc_s3_operations_total',
       'origin', 'op', 'status', job='${job_lifecycle_gc_processor}', namespace='${namespace}',
    )

    DURATION = metrics.BucketMetric(
       's3_gc_duration_seconds',
       'origin', job='${job_lifecycle_gc_processor}', namespace='${namespace}'
    )


class BacklogMetrics:
    LATEST_PUBLISHED_MESSAGE_TS = metrics.Metric(
        's3_zenko_queue_latest_published_message_timestamp',
        'topic', 'partition', job=['$jobs'], namespace='${namespace}',
    )

    DELIVERY_REPORTS_TOTAL = metrics.CounterMetric(
        's3_zenko_queue_delivery_reports_total',
        'status', job=['$jobs'], namespace='${namespace}',
    )

    LATEST_CONSUMED_MESSAGE_TS, LATEST_CONSUME_EVENT_TS = [
        metrics.Metric(
            name, 'topic', 'partition', 'group', job=['$jobs'], namespace='${namespace}',
        )
        for name in [
            's3_zenko_queue_latest_consumed_message_timestamp',
            's3_zenko_queue_latest_consume_event_timestamp',
        ]
    ]

    REBALANCE_TOTAL = metrics.CounterMetric(
        's3_zenko_queue_rebalance_total',
        'topic', 'group', 'status', job=['$jobs'], namespace='${namespace}',
    )

    SLOW_TASKS = metrics.Metric(
        's3_zenko_queue_slowTasks_count',
        'topic', 'partition', 'group', job=['$jobs'], namespace='${namespace}',
    )

    TASK_PROCESSING_TIME = metrics.BucketMetric(
        's3_zenko_queue_task_processing_time_seconds',
        'topic', 'partition', 'group', 'error', job=['$jobs'], namespace='${namespace}',
    )


def relabel_job(*expr):
    # type: (*str) -> str
    return '\n'.join([
        'label_replace(',
        'label_replace(',
        *['  ' + e for e in expr],
        ', "job", "$1", "job", "${zenkoName}-backbeat-(?:lifecycle-)?(.*?)-headless")',
        ', "job", "$1-processor", "job", "(gc|transition)")',
    ])


def color_override(name, color):
    # type: (str, str) -> dict
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{
            "id": "color",
            "value": {"fixedColor": color, "mode": "fixed"}
        }],
    }


up = [
    Stat(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        reduceCalc="last",
        minValue='0',
        maxValue=replicas,
        noValue='0',
        targets=[
            Target(
                expr='sum(up{namespace="${namespace}", job="' + job + '"})',
            ),
        ],
        thresholdType='percentage',
        thresholds=[
            Threshold('red', 0, 0.0),
            Threshold('yellow', 1, 50.),
            Threshold('green', 2, 100.),
        ],
    )
    for title, job, replicas in [
        ('Conductor',            '${job_lifecycle_producer}',             '1'),
        ('Bucket Processor',     '${job_lifecycle_bucket_processor}',     '${bucket_processor_replicas}'),
        ('Expiration Processor', '${job_lifecycle_object_processor}',     '${object_processor_replicas}'),
        ('Transition Processor', '${job_lifecycle_transition_processor}', '${transition_processor_replicas}'),
        ('GC Processor',         '${job_lifecycle_gc_processor}',         '${gc_processor_replicas}'),
        ('Populator',            '${job_lifecycle_populator}',            '1'),
    ]
]

lifecycle_batch = Stat(
    title="Latest Batch Start Time",
    dataSource="${DS_PROMETHEUS}",
    reduceCalc="lastNotNull",
    format='dateTimeAsLocalNoDateIfToday',
    targets=[
        Target(
            expr='max(max_over_time(' + Metrics.LATEST_BATCH_START_TIME() + '[24h]) > 0)',
            instant=True,
        ),
    ],
    thresholds=[
        Threshold('#808080', 0, 0.0),
        Threshold('blue', 1, 0.0),
    ],
)

bucket_listing_success_rate, s3_success_rate = [
    GaugePanel(
        title=name,
        dataSource='${DS_PROMETHEUS}',
        calc='mean',
        decimals=2,
        format=UNITS.PERCENT_FORMAT,
        min=0,
        max=100,
        noValue='-',
        targets=[Target(
            expr='\n'.join([
                '100 / (1 + (sum(rate(' + failed + ')) or vector(0))',
                '           /',
                '           (sum(rate(' + succeeded + ')) > 0)',
                '      )',
            ]),
        )],
        thresholds=[
            Threshold('#808080',            0,  0.0),
            Threshold('red',                1,  0.0),
            Threshold('orange',             2, 95.0),
            Threshold('super-light-yellow', 3, 99.0),
            Threshold('light-green',        4, 99.5),
            Threshold('green',              5, 99.995),
        ]
    )
    for name, failed, succeeded in [
        ('Listings success rate', Metrics.BUCKET_LISTING_ERROR(),  Metrics.BUCKET_LISTING_SUCCESS()),
        ('S3 success rate',       Metrics.S3_OPS('status!="200"'), Metrics.S3_OPS('status="200"')),
    ]
]

s3_request_rate = Stat(
    title="S3 Requests",
    dataSource="${DS_PROMETHEUS}",
    colorMode='background',
    format=UNITS.REQUESTS_PER_SEC,
    reduceCalc="mean",
    targets=[
        Target(expr='sum(rate(' + Metrics.S3_OPS() + '))'),
    ],
    thresholds=[Threshold('dark-purple', 0, 0.)],
)

circuit_breaker = s3_circuit_breaker(job=['$jobs'])
circuit_breaker_over_time = s3_circuit_breaker_over_time(job=['$jobs'])
circuit_breaker_over_time.targets[0].expr = relabel_job(circuit_breaker_over_time.targets[0].expr)

ops_rate = [
    Stat(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        colorMode='background',
        format=UNITS.OPS_PER_SEC,
        noValue='-',
        reduceCalc="mean",
        targets=[
            Target(
                expr='sum(rate(' + Metrics.DURATION.count(f'type=~"{type}"') + '))',
            ),
        ],
        thresholds=[Threshold('semi-dark-blue', 0, 0.)],
    )
    for title, type in [
        ('Expiration', 'expiration|expiration:mpu'),
        ('Transition', 'transition'),
        ('Archive',    'archive|archive:gc'),
        ('Restore',    'restore'),
    ]
]

workflow_rate = TimeSeries(
    title='Rate over time',
    dataSource='${DS_PROMETHEUS}',
    decimals=0,
    fillOpacity=30,
    legendCalcs=['mean'],
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.OPS_PER_SEC,
    lineInterpolation='smooth',
    targets=[
        Target(
            expr='sum(rate(' + Metrics.DURATION.count() + ')) by (type)',
            legendFormat="{{type}}",
        ),
        Target(
            expr='sum(rate(' + GcMetrics.DURATION.count() + '))',
            legendFormat="gc",
        )
    ],
)

trigger_latency, workflow_latency, workflow_duration_by_type, workflow_duration_by_location = [
    TimeSeries(
        title=title,
        dataSource='${DS_PROMETHEUS}',
        decimals=0,
        fillOpacity=30,
        legendCalcs=['max'],
        legendDisplayMode='table',
        legendPlacement='right',
        unit=UNITS.SECONDS,
        lineInterpolation='smooth',
        targets=[
            Target(
                expr='histogram_quantile(0.95, sum(rate(' + metric.bucket() + ')) by (le, ' + label + '))',
                legendFormat='{{' + label + '}}',
            ),
        ] + ([
            Target(
                expr='histogram_quantile(0.95, sum(rate(' + GcMetrics.DURATION.bucket() + ')) by (le))',
                legendFormat="gc",
            ),
        ] if title == 'Duration by Type' else []),
    )
    for title, metric, label in [
        ('Trigger Latency',      Metrics.TRIGGER_LATENCY, 'type'),
        ('Latency over time',    Metrics.LATENCY,         'type'),
        ('Duration by Type',     Metrics.DURATION,        'type'),
        ('Duration by Location', Metrics.DURATION,        'location'),
    ]
]

latency_distribution, expiration_distribution, transition_distribution, archive_distribution, restore_distribution = [
    Heatmap(
        title=title,
        dataSource='${DS_PROMETHEUS}',
        dataFormat='tsbuckets',
        hideZeroBuckets=True,
        maxDataPoints=25,
        tooltip=Tooltip(show=True, showHistogram=True),
        yAxis=YAxis(format=UNITS.SECONDS, decimals=0),
        cards={'cardPadding': 1, 'cardRound': 2},
        color=HeatmapColor(mode='opacity'),
        targets=[Target(
            expr='sum(increase(' + metric + ')) by(le)',
            format="heatmap",
            legendFormat="{{le}}",
        )],
    )
    for title, metric in [
        ('Latency Distribution',             Metrics.LATENCY.bucket()),
        ('Expiration Duration Distribution', Metrics.DURATION.bucket(type='expiration')),
        ('Transition Duration Distribution', Metrics.DURATION.bucket(type='transition')),
        ('Archive Duration Distribution',    Metrics.DURATION.bucket(type='archive')),
        ('Restore Duration Distribution',    Metrics.DURATION.bucket(type='restore')),
    ]
]

workflow_by_location, workflow_by_type = [
    PieChart(
        title=title,
        dataSource='${DS_PROMETHEUS}',
        legendDisplayMode='hidden',
        pieType='donut',
        reduceOptionsCalcs=['mean'],
        unit=UNITS.OPS_PER_SEC,
        targets=[
            Target(
                expr='sum(rate(' + Metrics.DURATION.count() + ')) by ('+ label + ')',
                legendFormat='{{' + label + '}}',
            ),
        ]
    )
    for title, label in [
        ("By location", 'location'),
        ("By type",     'type'),
    ]
]

s3_delete_object_ops, s3_delete_mpu_ops = [
    TimeSeries(
        title=f'{op} Request Rate',
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        lineInterpolation='smooth',
        unit=UNITS.REQUESTS_PER_SEC,
        stacking={"mode": "normal", "group": "A"},
        targets=[
            Target(
                expr='sum(rate(' + Metrics.S3_OPS('status!="200"', op=op, job="${job_lifecycle_object_processor}") + '))',
                legendFormat="Error",
            ),
            Target(
                expr='sum(rate(' + Metrics.S3_OPS(status=200, op=op, job="${job_lifecycle_object_processor}") + '))',
                legendFormat="Success",
            ),
        ],
        overrides=[
            color_override("Error",   "red"),
            color_override("Success", "green"),
        ],
    )
    for op in ['deleteObject', 'abortMultipartUpload']
]

messages_published_by_op = TimeSeries(
    title='Message published',
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=5,
    lineInterpolation='smooth',
    unit=UNITS.REQUESTS_PER_SEC,
    targets=[
        Target(
            expr='sum(rate(' + Metrics.KAFKA_PUBLISH_SUCCESS() + ')) by(op)',
            legendFormat="{{ op }}",
        ),
    ],
)

# Basically a wrapper for KAFKA_PUBLISH_ERROR' rate, but filling the gaps with 0,
# while keeping the same labels.
kafka_publish_error_rate_expr = '\n'.join([
    '(sum(rate(' + Metrics.KAFKA_PUBLISH_ERROR() + ')) by(op)',
    '  or 0 * group(' + Metrics.KAFKA_PUBLISH_SUCCESS.raw() + ') by(op))',
])

failed_publish_by_op = TimeSeries(
    title='Publish errors',
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=5,
    lineInterpolation='smooth',
    unit=UNITS.REQUESTS_PER_SEC,
    targets=[
        Target(
            expr=kafka_publish_error_rate_expr,
            legendFormat="{{ op }}",
        ),
    ],
)

publish_success_rate = Stat(
    title='Publish success rate',
    dataSource="${DS_PROMETHEUS}",
    decimals=2,
    format=UNITS.PERCENT_FORMAT,
    graphMode='none',
    orientation='horizontal',
    reduceCalc='mean',
    textMode='value_and_name',
    targets=[
        Target(
            expr='\n'.join([
                '100 / (1 + ' + kafka_publish_error_rate_expr.replace('\n', '\n' +
                '           '),
                '           /',
                '           (sum(rate(' + Metrics.KAFKA_PUBLISH_SUCCESS() + ')) by(op) > 0))'
            ]),
            legendFormat="{{ op }}",
        )
    ],
    thresholdType='percentage',
    thresholds=[
        Threshold("green",  0, 0.0),
        Threshold("yellow", 0, 0.1),
        Threshold("orange", 0, 0.01),
        Threshold("red",    1, 1.0),
    ],
)

kafka_lag = TimeSeries(
    title='Kafka Lag',
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation='smooth',
    unit=UNITS.SHORT,
    targets=[
        Target(
            expr=relabel_job('\n'.join([
                'sum(',
                '   kafka_consumergroup_group_max_lag',
                '   * on(group) group_right',
                '   group(' + BacklogMetrics.LATEST_CONSUMED_MESSAGE_TS() + ') by(group, job)',
                ') by(job)',
            ])),
            legendFormat="{{ job }}",
        )
    ],
)

tasks_latency = TimeSeries(
    title='Tasks latency',
    dataSource="${DS_PROMETHEUS}",
    unit=UNITS.MILLI_SECONDS,
    legendDisplayMode='table',
    legendValues=['min', 'mean', 'max'],
    legendPlacement='right',
    lineInterpolation='smooth',
    targets=[
        Target(
            expr=relabel_job(
                f'max({BacklogMetrics.LATEST_CONSUME_EVENT_TS()} - {BacklogMetrics.LATEST_CONSUMED_MESSAGE_TS()}) by(job)'
            ),
            legendFormat="{{ job }}",
        )
    ]
)

avg_task_processing_time = TimeSeries(
    title='Average task processing time',
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation='smooth',
    unit=UNITS.SECONDS,
    targets=[
        Target(
            expr=relabel_job(
                'sum(rate(' + BacklogMetrics.TASK_PROCESSING_TIME.sum() + ')) by (job)',
                '/'
                'sum(rate(' + BacklogMetrics.TASK_PROCESSING_TIME.count() + ')) by (job)',
            ),
            legendFormat="{{ job }}",
        )
    ]
)

task_processing_time_distribution = Heatmap(
    title='Task processing time',
    dataSource="${DS_PROMETHEUS}",
    dataFormat='tsbuckets',
    hideZeroBuckets=True,
    maxDataPoints=30,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SECONDS, decimals=0),
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='sum(increase(' + BacklogMetrics.TASK_PROCESSING_TIME.bucket() + ')) by(le)',
        format="heatmap",
        legendFormat="{{ le }}",
    )],
)

slow_tasks_over_time = TimeSeries(
    title='Slow tasks',
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation='smooth',
    unit=UNITS.SHORT,
    targets=[
        Target(
            expr=relabel_job(
                'sum(' + BacklogMetrics.SLOW_TASKS() + ') by (job)'
            ),
            legendFormat="{{ job }}",
        )
    ]
)

rebalance_over_time = TimeSeries(
    title='Rebalance',
    dataSource="${DS_PROMETHEUS}",
    lineInterpolation='smooth',
    unit=UNITS.SHORT,
    targets=[
        Target(
            expr=relabel_job(
                'sum(increase(' + BacklogMetrics.REBALANCE_TOTAL() + ')) by (job)'
            ),
            legendFormat="{{ job }}",
        )
    ],
)

s3_requests_status = TimeSeries(
    title='S3 Requests status over time',
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=30,
    lineInterpolation="smooth",
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.S3_OPS() + ')) by (status)',
        legendFormat="{{ status }}",
    )],

)

s3_requests_aggregated_status = TimeSeries(
    title='S3 Requests Aggregated status over time',
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=39,
    lineInterpolation='smooth',
    scaleDistributionType="log",
    stacking={"mode": "normal", "group": "A"},
    tooltipMode='multi',
    unit=UNITS.OPS_PER_SEC,
    targets=[
        Target(
            expr='sum(rate(' + Metrics.S3_OPS(f'status=~"{status}"') + '))',
            legendFormat=name,
        )
        for name, status in [
            ("Success", STATUS_CODE_2XX),
            ("User errors", STATUS_CODE_4XX),
            ("System errors", STATUS_CODE_5XX),
        ]
    ],
    overrides=[
        color_override("Success", "dark-blue"),
        color_override("User errors", "semi-dark-orange"),
        color_override("System errors", "semi-dark-red"),
    ],
)

s3_requests_error_rate = Stat(
        title='Error rate',
        dataSource="${DS_PROMETHEUS}",
        format=UNITS.PERCENT_UNIT,
        orientation='horizontal',
        reduceCalc='mean',
        targets=[
            Target(
                expr='\n'.join([
                    '(sum(rate(' + Metrics.S3_OPS(status) + ')) or vector(0))',
                    ' /',
                    '(sum(rate('  + Metrics.S3_OPS() + ')) > 0)',
                ]),
                legendFormat=title,
            )
            for title, status in [
                ("All errors", 'status!="200"'),
                ("User errors", f'status=~"{STATUS_CODE_4XX}"'),
                ("System errors", f'status=~"{STATUS_CODE_5XX}"'),
            ]
        ],
        overrides=[
            color_override("All errors", "dark-blue"),
            color_override("User errors", "semi-dark-orange"),
            color_override("System errors", "semi-dark-red"),
        ],
    )

s3_requests_by_workflow = TimeSeries(
    title="S3 Request rate per workflow",
    dataSource="${DS_PROMETHEUS}",
    legendDisplayMode="table",
    legendPlacement="right",
    legendValues=["min", "mean", "max"],
    lineInterpolation="smooth",
    unit=UNITS.OPS_PER_SEC,
    targets=[
        Target(
            expr='sum(rate(' + Metrics.S3_OPS() + ')) by(origin)',
            legendFormat="{{ origin }}",
        )
    ]
)

s3_requests_errors_by_workflow = PieChart(
    title="S3 errors distribution",
    dataSource="${DS_PROMETHEUS}",
    legendDisplayMode='table',
    legendPlacement='right',
    legendValues=['percent'],
    pieType='donut',
    reduceOptionsCalcs=['mean'],
    unit=UNITS.OPS_PER_SEC,
    targets=[
        Target(
            expr='sum(rate(' + Metrics.S3_OPS('status!="200"') + ')) by(origin)',
            legendFormat="{{ origin }}",
        ),
    ]
)

lifecycle_scans = TimeSeries(
    title="Lifecycle Scan Period",
    dataSource="${DS_PROMETHEUS}",
    drawStyle='bars',
    fillOpacity=50,
    legendDisplayMode="hidden",
    lineWidth=0,
    lineInterpolation="smooth",
    unit=UNITS.MILLI_SECONDS,
    targets=[
        Target(
            expr='\n'.join([
                'max(',
                '  increase(' + Metrics.LATEST_BATCH_START_TIME() + '[$__rate_interval])',
                '  /',
                '  changes(' + Metrics.LATEST_BATCH_START_TIME() + '[$__rate_interval] offset 30s)',
                ') > 0',
            ]),
            legendFormat='Scan',
        ),
    ],
)

lifecycle_scan_rate = TimeSeries(
    title="Lifecycle Scan Rate",
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=50,
    lineInterpolation="smooth",
    stacking={"mode": "normal", "group": "A"},
    unit=UNITS.OPS_PER_MIN,
    targets=[
        Target(
            expr='sum(irate(' + metric() + ')) * 60',
            legendFormat=label,
        )
        for metric, label in [
            (Metrics.BUCKET_LISTING_ERROR,      'Error'),
            (Metrics.BUCKET_LISTING_THROTTLING, 'Throttling'),
            (Metrics.BUCKET_LISTING_SUCCESS,    'Success'),
        ]
    ],
    overrides=[
        color_override("Error",      "red"),
        color_override("Throttling", "blue"),
        color_override("Success",    "green"),
    ]
)

active_indexing_jobs = TimeSeries(
    title="Active Indexing jobs",
    dataSource="${DS_PROMETHEUS}",
    legendDisplayMode="hidden",
    unit=UNITS.SHORT,
    targets=[
        Target(
            expr='sum(' + Metrics.ACTIVE_INDEXING_JOBS() + ') or vector(0)',
        ),
    ],
)

legacy_tasks = TimeSeries(
    title="Legacy Tasks",
    dataSource="${DS_PROMETHEUS}",
    fillOpacity=5,
    legendDisplayMode="table",
    legendPlacement="right",
    lineInterpolation="smooth",
    unit=UNITS.SHORT,
    targets=[
        Target(
            expr='sum(increase(' + Metrics.LEGACY_TASKS() + ')) by(status)',
            legendFormat="{{ status }}",
        ),
    ],
)


dashboard = (
    Dashboard(
        title="Lifecycle",
        editable=True,
        refresh="30s",
        tags=["backbeat", "lifecycle"],
        timezone="",
        inputs=[
            DataSourceInput(
                name="DS_PROMETHEUS",
                label="Prometheus",
                pluginId="prometheus",
                pluginName="Prometheus",
            ),
            ConstantInput(
                name="namespace",
                label="namespace",
                description="Namespace associated with the Zenko instance",
                value="zenko",
            ),
            ConstantInput(
                name="zenkoName",
                label="zenko instance name",
                description="Name of the Zenko instance",
                value="artesca-data",
            ),
            ConstantInput(
                name="job_lifecycle_producer",
                label="job lifecycle producer",
                description="Name of the lifecycle conductor job, used to filter only lifecycle conductor instances",
                value="artesca-data-backbeat-lifecycle-producer-headless",
            ),
            ConstantInput(
                name="job_lifecycle_bucket_processor",
                label="job lifecycle bucket processor",
                description="Name of the lifecycle bucket processor job, used to filter only lifecycle bucket processor instances",
                value="artesca-data-backbeat-lifecycle-bucket-processor-headless",
            ),
            ConstantInput(
                name="job_lifecycle_object_processor",
                label="job lifecycle object processor",
                description="Name of the lifecycle object processor job, used to filter only lifecycle object processor instances",
                value="artesca-data-backbeat-lifecycle-object-processor-headless",
            ),
            ConstantInput(
                name="job_lifecycle_transition_processor",
                label="job lifecycle transition processor",
                description="Name of the lifecycle transition processor job, used to filter only lifecycle transition processor instances",
                value="artesca-data-backbeat-lifecycle-transition-headless",
            ),
            ConstantInput(
                name="job_sorbet_forwarder",
                label="job sorbet forwarder",
                description="Prefix of the sorbet forwarder jobs, used to filter only sorbet forwarder instances",
                value="artesca-data-backbeat-cold-sorbet-fwd",
            ),
            ConstantInput(
                name="job_lifecycle_populator",
                label="job lifecycle populator",
                description="Name of the lifecycle populator job, used to filter only lifecycle populator instances",
                value="artesca-data-backbeat-lifecycle-populator-headless",
            ),
            ConstantInput(
                name="job_lifecycle_gc_processor",
                label="job lifecycle gc processor",
                description="Name of the lifecycle gc processor job, used to filter only lifecycle gc processor instances",
                value="artesca-data-backbeat-gc-headless",
            ),
            ConstantInput(
                name="bucket_processor_replicas",
                label="bucket processor replicas",
                description="Number of bucket processor replicas",
                value="1",
            ),
            ConstantInput(
                name="object_processor_replicas",
                label="object processor replicas",
                description="Number of object processor replicas",
                value="1",
            ),
            ConstantInput(
                name="transition_processor_replicas",
                label="transition processor replicas",
                description="Number of transition processor replicas",
                value="1",
            ),
            ConstantInput(
                name="gc_processor_replicas",
                label="gc processor replicas",
                description="Number of gc processor replicas",
                value="1",
            ),
        ],
        templating=Templating([
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Service',
                name='jobs',
                query=f'label_values(up{{namespace="${{namespace}}", job=~"{"|".join(ALL_JOBS)}"}}, job)',
                regex='/^${zenkoName}-(?:backbeat-|cold-sorbet-)?(?:lifecycle-)?(.*?)(?:-processor)?(?:-headless)?$/',
                includeAll=True,
                multi=True,
                allValue="|".join(ALL_JOBS),
            ),
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Location',
                name='locations',
                query=f'label_values({{namespace="${{namespace}}", job=~"{"|".join(ALL_JOBS)}", location!=""}}, location)',
                includeAll=True,
                multi=True,
                allValue='.*',
                refresh=REFRESH_ON_TIME_RANGE_CHANGE,
            ),
        ]),
        panels=layout.column([
            layout.row(up + layout.resize([lifecycle_batch], width=6), height=4),
            layout.row([
                circuit_breaker, bucket_listing_success_rate, *ops_rate, s3_request_rate, s3_success_rate,
            ], height=4),
            layout.row([circuit_breaker_over_time], height=5),
            RowPanel(title="Lifecycle Workflows"),
            layout.row([workflow_rate, *layout.resize([workflow_by_type, workflow_by_location], width=5)], height=7),
            layout.row([workflow_latency, latency_distribution], height=7),
            layout.row([workflow_duration_by_type, workflow_duration_by_location], height=7),
            layout.row([expiration_distribution, transition_distribution], height=7),
            layout.row([archive_distribution, restore_distribution], height=7),
            RowPanel(title="Lifecycle Tasks"),
            layout.row([messages_published_by_op, failed_publish_by_op, *layout.resize([publish_success_rate], width=4)], height=7),
            layout.row([*layout.resize([kafka_lag], width=10), tasks_latency], height=7),
            layout.row([avg_task_processing_time, task_processing_time_distribution], height=7),
            layout.row([slow_tasks_over_time, rebalance_over_time], height=7),
            RowPanel(title="S3 Operations"),
            layout.row([s3_requests_status, s3_requests_aggregated_status, *layout.resize([s3_requests_error_rate], width=2)], height=8),
            layout.row([s3_requests_by_workflow, *layout.resize([s3_requests_errors_by_workflow], width=7)], height=8),
            layout.row([s3_delete_object_ops, s3_delete_mpu_ops], height=8),
            RowPanel(title="Lifecycle Conductor"),
            layout.row([lifecycle_scans, trigger_latency], height=7),
            layout.row([lifecycle_scan_rate, active_indexing_jobs, legacy_tasks], height=7),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
