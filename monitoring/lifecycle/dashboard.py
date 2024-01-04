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
from scalgrafanalib import layout, Stat, Target, TimeSeries, Dashboard

import os, sys
sys.path.append(os.path.abspath(f'{__file__}/../../..'))
from monitoring import s3_circuit_breaker


STATUS_CODE_2XX = '2..'
STATUS_CODE_3XX = '3..'
STATUS_CODE_4XX = '4..'
STATUS_CODE_5XX = '5..'


def s3_request_timeseries_expr(process, job, code):
    labelSelector = 'namespace="${namespace}"'
    labelSelector += f',status=~"{code}"'

    if job is not None:
        labelSelector += f',job="{job}"'

    if process is not None:
        labelSelector += f',origin="{process}"'

    return f'sum(increase(s3_lifecycle_s3_operations_total{{{labelSelector}}}[$__rate_interval]))'


def s3_request_timeseries(title, process=None, job=None):
    return TimeSeries(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        legendDisplayMode='table',
        targets=[
            Target(
                expr=s3_request_timeseries_expr(process, job, STATUS_CODE_2XX),
                legendFormat="HTTP 2xx",
            ),
            Target(
                expr=s3_request_timeseries_expr(process, job, STATUS_CODE_3XX),
                legendFormat="HTTP 3xx",
            ),
            Target(
                expr=s3_request_timeseries_expr(process, job, STATUS_CODE_4XX),
                legendFormat="HTTP 4xx",
            ),
            Target(
                expr=s3_request_timeseries_expr(process, job, STATUS_CODE_5XX),
                legendFormat="HTTP 5xx",
            ),
        ]
    )


def s3_request_error_rate_expr(process, job, code):
    divdLabel = 'namespace="${namespace}"'
    divsLabel = 'namespace="${namespace}"'

    if code is not None:
        divdLabel += f',status=~"{code}"'
    else:
        divdLabel += ',status!="200"'

    if job is not None:
        divdLabel += f',job="{job}"'
        divsLabel += f',job="{job}"'

    if process is not None:
        divdLabel += f',origin="{process}"'
        divsLabel += f',origin="{process}"'

    divd = f'sum(rate(s3_lifecycle_s3_operations_total{{{divdLabel}}}[$__rate_interval]))'
    divs = f'sum(rate(s3_lifecycle_s3_operations_total{{{divsLabel}}}[$__rate_interval]) > 0)'

    return f'{divd}/{divs}'


def s3_request_error_rate(title, process=None, job=None, code=None):
    return Stat(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        format=UNITS.PERCENT_UNIT,
        reduceCalc="mean",
        targets=[
            Target(expr=s3_request_error_rate_expr(process, job, code)),
        ],
        thresholds=[
            Threshold("green", 0, 0.0),
            Threshold("red", 1, 0.05),
        ],
    )


def s3_request_error_rates(process=None, job=None):
    return [
        s3_request_error_rate( "S3 All Errors", process=process, job=job),
        s3_request_error_rate( "S3 3xx Errors", process=process, job=job, code=STATUS_CODE_3XX),
        s3_request_error_rate( "S3 4xx Errors", process=process, job=job, code=STATUS_CODE_4XX),
        s3_request_error_rate( "S3 5xx Errors", process=process, job=job, code=STATUS_CODE_5XX),
    ]


def s3_deletion_request_time_series(op):
    successLabel = f'status="200",op="{op}",namespace="{"${namespace}"}",job="{"${job_lifecycle_object_processor}"}"'
    errorLabel = f'status!="200",op="{op}",namespace="{"${namespace}"}",job="{"${job_lifecycle_object_processor}"}"'

    return TimeSeries(
        title=f'{op} Request Rate',
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        unit=UNITS.REQUESTS_PER_SEC,
        targets=[
            Target(
                expr=f'sum(rate(s3_lifecycle_s3_operations_total{{{successLabel}}}[$__rate_interval]))',
                legendFormat="success",
            ),
            Target(
                expr=f'sum(rate(s3_lifecycle_s3_operations_total{{{errorLabel}}}[$__rate_interval]))',
                legendFormat="error",
            ),
        ],
    )


def kafka_messages_time_series(title, expr):
    return TimeSeries(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        fillOpacity=5,
        scaleDistributionType='log',
        scaleDistributionLog=10,
        legendDisplayMode='hidden',
        targets=[
            Target(
                expr=expr,
                legendFormat="messages",
            ),
        ],
    )


def kafka_row(topic, op):
    label = f'op="{op}",namespace="{"${namespace}"}"'
    return [
        kafka_messages_time_series(
            f'{topic} Messages in Queue',
            f'sum(increase(s3_lifecycle_kafka_publish_success_total{{{label}}}[$__rate_interval]))',
        ),
        kafka_messages_time_series(
            f'{topic} Failed Messages',
            f'sum(increase(s3_lifecycle_kafka_publish_error_total{{{label}}}[$__rate_interval]))',
        ),
    ]


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
                expr='sum(up{namespace="${namespace}",job="' + job + '})',
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
    format='dateTimeAsLocal',
    targets=[
        Target(
            expr='s3_lifecycle_latest_batch_start_time{job="${job_lifecycle_producer}",namespace="${namespace}"}',
            instant=True,
        ),
    ],
)

lifecycle_global_s3_requests = s3_request_timeseries("S3 Requests")
lifecycle_global_s3_error_rates = s3_request_error_rates()

lifecycle_bucket_processor_s3_requests = s3_request_timeseries(
    "S3 Requests",
    process="bucket",
    job='${job_lifecycle_bucket_processor}',
)

lifecycle_bucket_processor_s3_error_rates = s3_request_error_rates(
    process="bucket",
    job='${job_lifecycle_bucket_processor}',
)

lifecycle_expiration_processor_s3_requests = s3_request_timeseries(
    "S3 Requests",
    process="expiration",
    job='${job_lifecycle_object_processor}',
)

lifecycle_expiration_processor_s3_error_rates = s3_request_error_rates(
    process="expiration",
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

lifecycle_expiration_processor_s3_delete_object_ops = s3_deletion_request_time_series("deleteObject")
lifecycle_expiration_processor_s3_delete_mpu_ops = s3_deletion_request_time_series("abortMultipartUpload")

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
            layout.row(up, height=4),
            layout.row([lifecycle_global_s3_requests], height=10),
            layout.row(lifecycle_global_s3_error_rates, height=4),
            RowPanel(title="Kafka"),
            layout.row(kafka_row("Lifecycle Bucket Task", "BucketTopic"), height=10),
            layout.row(kafka_row("Expiration Object Task", "ObjectTopic"), height=10),
            RowPanel(title="Lifecycle Conductor"),
            layout.row([lifecycle_batch], height=4),
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
