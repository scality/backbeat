from grafanalib.core import (
    BarGauge,
    ConstantInput,
    DataSourceInput,
    GaugePanel,
    Heatmap,
    HeatmapColor,
    RowPanel,
    Threshold,
    YAxis,
)

from grafanalib import formatunits as UNITS
from scalgrafanalib import layout, metrics, GaugePanel, Stat, Target, TimeSeries, Dashboard

import os, sys
sys.path.append(os.path.abspath(f'{__file__}/../../..'))
from monitoring import s3_circuit_breaker


STATUS_CODE_2XX = '2..'
STATUS_CODE_3XX = '3..'
STATUS_CODE_4XX = '4..'
STATUS_CODE_5XX = '5..'

class Metrics:
    LATEST_BATCH_START_TIME = metrics.Metric(
        's3_lifecycle_latest_batch_start_time',
        job='${job_lifecycle_producer}', namespace='${namespace}',
    )

    BUCKET_LISTING_SUCCESS, BUCKET_LISTING_ERROR = [
        metrics.CounterMetric(
            name, job='${job_lifecycle_producer}', namespace='${namespace}',
        )
        for name in [
            's3_lifecycle_conductor_bucket_list_success_total',
            's3_lifecycle_conductor_bucket_list_error_total',
        ]
    ]

    S3_OPS = metrics.CounterMetric(
       's3_lifecycle_s3_operations_total',
       'origin', 'op', 'status', 'job', namespace='${namespace}',
    )

    TRIGGER_LATENCY, LATENCY, DURATION = [
        metrics.BucketMetric(
            name, 'location', 'type', 'job', namespace='${namespace}',
        )
        for name in [
            's3_lifecycle_trigger_latency_seconds',
            's3_lifecycle_latency_seconds',
            's3_lifecycle_duration_seconds',
        ]
    ]

    KAFKA_PUBLISH_SUCCESS, KAFKA_PUBLISH_ERROR = [
        metrics.CounterMetric(
            name, 'origin', 'op', 'job', namespace='${namespace}',
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


def s3_request_timeseries(title, **kwargs):
    return TimeSeries(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        legendDisplayMode='table',
        unit=UNITS.REQUESTS_PER_SEC,
        targets=[
            Target(
                expr='sum(rate(' + Metrics.S3_OPS(f'status=~"{status}"', **kwargs) + '))',
                legendFormat=name,
            )
            for name, status in [
                ("Success", STATUS_CODE_2XX),
                ("User errors", STATUS_CODE_4XX),
                ("System errors", STATUS_CODE_5XX),
            ]
        ]
    )


def s3_request_error_rates(**kwargs):
    return [
        Stat(
            title=title,
            dataSource="${DS_PROMETHEUS}",
            format=UNITS.PERCENT_UNIT,
            reduceCalc="mean",
            targets=[
                Target(expr='\n'.join([
                    'sum(rate(' + Metrics.S3_OPS(status, **kwargs) + '))',
                    '/',
                    'sum(rate('  + Metrics.S3_OPS(**kwargs) + ') > 0)',
                ])),
            ],
            thresholds=[
                Threshold("green", 0, 0.0),
                Threshold("red", 1, 0.05),
            ],
        )
        for title, status in [
            ("Error rate", 'status!="200"'),
            ("User Error rate", f'status=~"{STATUS_CODE_4XX}"'),
            ("System Error rate", f'status=~"{STATUS_CODE_5XX}"'),
        ]
    ]


def kafka_message_produced(title, op):
    return TimeSeries(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        unit=UNITS.REQUESTS_PER_SEC,
        targets=[
            Target(
                expr='sum(rate(' + Metrics.KAFKA_PUBLISH_SUCCESS(op=op) + '))',
                legendFormat="Successful",
            ),
            Target(
                expr='sum(rate(' + Metrics.KAFKA_PUBLISH_ERROR(op=op) + '))',
                legendFormat="Failed",
            ),
        ],
    )


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

circuit_breaker = s3_circuit_breaker(
    'Flow Control',
    job=[
        '${job_lifecycle_producer}',
        '${job_lifecycle_bucket_processor}',
        '${job_lifecycle_object_processor}',
        '${job_lifecycle_transition_processor}',
        '${job_lifecycle_gc_processor}',
        '${job_lifecycle_populator}',
    ],
)

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

lifecycle_global_s3_requests = s3_request_timeseries("S3 Requests")
lifecycle_global_s3_error_rates = s3_request_error_rates()

lifecycle_bucket_processor_s3_requests = s3_request_timeseries(
    "S3 Requests",
    origin="bucket",
    job='${job_lifecycle_bucket_processor}',
)

lifecycle_bucket_processor_s3_error_rates = s3_request_error_rates(
    origin="bucket",
    job='${job_lifecycle_bucket_processor}',
)

lifecycle_expiration_processor_s3_requests = s3_request_timeseries(
    "S3 Requests",
    origin="expiration",
    job='${job_lifecycle_object_processor}',
)

lifecycle_expiration_processor_s3_error_rates = s3_request_error_rates(
    origin="expiration",
    job='${job_lifecycle_object_processor}',
)

lifecycle_conductor_circuit_breaker = s3_circuit_breaker(
    'Flow Control',
    process='producer',
    job='${job_lifecycle_producer}',
)

lifecycle_bucket_processor_circuit_breaker = s3_circuit_breaker(
    'Flow Control',
    process='bucket',
    job='${job_lifecycle_bucket_processor}',
)

lifecycle_object_processor_circuit_breaker = s3_circuit_breaker(
    'Flow Control',
    process='expiration',
    job='${job_lifecycle_object_processor}',
)

lifecycle_expiration_processor_s3_delete_object_ops, lifecycle_expiration_processor_s3_delete_mpu_ops = [
    TimeSeries(
        title=f'{op} Request Rate',
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        unit=UNITS.REQUESTS_PER_SEC,
        targets=[
            Target(
                expr='sum(rate(' + Metrics.S3_OPS(status=200, op=op, job="${job_lifecycle_object_processor}") + '))',
                legendFormat="success",
            ),
            Target(
                expr='sum(rate(' + Metrics.S3_OPS('status!="200"', op=op, job="${job_lifecycle_object_processor}") + '))',
                legendFormat="error",
            ),
        ],
    )
    for op in ['deleteObject', 'abortMultipartUpload']
]

dashboard = (
    Dashboard(
        title="Backbeat Lifecycle",
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
                value="default",
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
        panels=layout.column([
            layout.row(up + layout.resize([lifecycle_batch], width=6), height=4),
            layout.row([
                circuit_breaker, bucket_listing_success_rate, *ops_rate, s3_request_rate, s3_success_rate,
            ], height=4),
            layout.row([lifecycle_global_s3_requests], height=10),
            layout.row(lifecycle_global_s3_error_rates, height=4),
            RowPanel(title="Kafka Message Produced"),
            layout.row([
                kafka_message_produced("Bucket Task", "BucketTopic"),
                kafka_message_produced("Expiration Task", "ObjectTopic"),
            ], height=8),
            layout.row([
                kafka_message_produced("Data Mover Task", "DataMoverTopic"),
                kafka_message_produced("Gc Task", "GcTopic"),
            ], height=8),
            layout.row([
                kafka_message_produced("Cold Storage Archive", "ColdStorageArchiveTopic"),
                kafka_message_produced("Cold Storage Gc", "ColdStorageGcTopic"),
            ], height=8),
            layout.row([
                kafka_message_produced("Cold Storage Restore", "ColdStorageRestoreTopic"),
                kafka_message_produced("Cold Storage Restore Adjust", "ColdStorageRestoreAdjustTopic"),
            ], height=8),
            RowPanel(title="Lifecycle Conductor"),
            layout.row([lifecycle_conductor_circuit_breaker], height=10),
            RowPanel(title="Lifecycle Bucket Processors"),
            layout.row([lifecycle_bucket_processor_s3_requests], height=10),
            layout.row(lifecycle_bucket_processor_s3_error_rates, height=4),
            layout.row([lifecycle_bucket_processor_circuit_breaker], height=10),
            RowPanel(title="Lifecycle Expiration Processors"),
            layout.row([lifecycle_expiration_processor_s3_requests], height=10),
            layout.row(lifecycle_expiration_processor_s3_error_rates, height=4),
            layout.row([lifecycle_expiration_processor_s3_delete_object_ops], height=10),
            layout.row([lifecycle_expiration_processor_s3_delete_mpu_ops], height=10),
            layout.row([lifecycle_object_processor_circuit_breaker], height=10),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
