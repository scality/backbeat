from grafanalib.core import (
    ConstantInput,
    DataSourceInput,
    RowPanel,
    Threshold,
    StateTimeline,
    Templating,
    Template,
    Heatmap,
    HeatmapColor,
    YAxis,
)
from grafanalib import formatunits as UNITS
from scalgrafanalib import(
    layout,Target,
    TimeSeries,
    Dashboard,
    Stat,
    GaugePanel,
    metrics,
    Tooltip,
)

LABEL_EXPR = {
    'notification-processors': '.*-(.*)-headless',
    'topics': '.*backbeat-notification-(.*).*'
}

DEFAULT_JOB_PREFIX = 'artesca-data-'

class Metrics:
    DELIVERY_LAG = metrics.BucketMetric(
        'notification_queue_processor_notification_delivery_delay_sec',
        'target', 'job', 'status', namespace='${namespace}',
    ).with_defaults(
        'job=~"${notification_processor}"',
        'target=~"${target}"',
    )

    CONSUMED_EVENTS = metrics.CounterMetric(
        'notification_queue_populator_event',
        job='${job_notification_producer}', namespace='${namespace}'
    )

    NOTIF_SIZE, PROCESSOR_EVENTS = [
        metrics.CounterMetric(
            name, 'job', 'target', *extraLabels, namespace='${namespace}'
        ).with_defaults(
            'job=~"${notification_processor}"',
            'target=~"${target}"',
        ) for name, extraLabels in {
            'notification_queue_processor_notification_size': [],
            'notification_queue_processor_event': ['eventType'],
        }.items()
    ]

    CACHE_LAG = metrics.BucketMetric(
        'notification_config_manager_config_get_sec',
        'cache_hit', 'job', namespace='${namespace}',
    ).with_defaults(
        'job=~"${notification_processor}|${job_notification_producer}"'
    ) 

    CACHE_UPDATES, CACHED_BUCKETS = [
        metrics.CounterMetric(
            name, 'job', *extraLabels, namespace='${namespace}'
        ).with_defaults(
            'job=~"${notification_processor}|${job_notification_producer}"'
        ) for name, extraLabels in {
            'notification_config_manager_cache_updates': ['op'],
            'notification_config_manager_cached_buckets_count': [],
        }.items()
    ]

producer_up_current = Stat(
    title='Notification producer',
    description='Number of active notification producer instances',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='0',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'sum(up{',
            '    namespace="${namespace}",',
            '    job="${job_notification_producer}"',
            '})',
        ]),
    )],
    thresholds=[
        Threshold('red', 0, 0.),
        Threshold('green', 1, 1.),
    ])

processors_up_current = StateTimeline(
    title='Notification Processors',
    description='Active notification processor instances',
    dataSource='${DS_PROMETHEUS}',
    legendDisplayMode='hidden',
    showValue='never',
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'sum by(job) (up{',
            '    namespace="${namespace}",',
            '    job=~"${notification_processor}"',
            '}), "job", "$1", "job", "' + LABEL_EXPR['notification-processors'] +'")',
        ]),
        legendFormat='{{job}}',
    )],
    thresholds=[
        Threshold('red', 0, 0.),
        Threshold('green', 1, 1.),
    ])

zookeeper_quorum = Stat(
    title='Zookeeper Quorum',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='0',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'max(',
            'quorum_size{',
            '  namespace="${namespace}",',
            '  job="${job_zookeeper}"',
            '})',
        ]),
    )],
    thresholds=[
        Threshold('green', 1, 0.),
    ])

kafka_brokers = Stat(
    title='Kafka Brokers',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='0',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'count(',
            'kafka_server_replicamanager_leadercount{',
            ' namespace="${namespace}",',
            ' job="${job_kafka}"',
            '})',
        ]),
    )],
    thresholds=[
        Threshold('green', 1, 0.),
    ])

kafka_lag = TimeSeries(
    title='Notification Topics Lag',
    description='Consumer lag of each of the notification topics',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'max by(topic) (',
            '    kafka_consumergroup_group_lag_seconds{'
            '       topic=~"${notification_topics}"',
            '}), "topic", "$1", "topic", "' + LABEL_EXPR['topics'] +'")',
        ]),
        legendFormat='{{topic}}',
    )])

success_rate = GaugePanel(
    title='Success Rate',
    dataSource='${DS_PROMETHEUS}',
    min=0,
    max=1,
    noValue='-',
    format=UNITS.PERCENT_UNIT,
    targets=[Target(
        expr='\n'.join([
            'sum(rate(' + Metrics.DELIVERY_LAG.count(status='success')+ '))',
            '/',
            'sum(rate(' + Metrics.DELIVERY_LAG.count('status=~".*"') + '))',
        ]),
    )],
    thresholds=[
        Threshold('red', 1, 0.),
        Threshold('yellow', 2, 0.8),
        Threshold('green', 3, 0.99),
    ])

notification_rate = Stat(
    title='Notification Rate',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='0',
    reduceCalc='last',
    format=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.DELIVERY_LAG.count(status='success') + '))',
    )],
    thresholds=[
        Threshold('green', 1, 0.),
    ])

bandwidth_rate = Stat(
    title='Bandwidth Rate',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='0',
    reduceCalc='last',
    format=UNITS.BYTES_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.NOTIF_SIZE() + '))',
    )],
    thresholds=[
        Threshold('green', 1, 0.),
    ])

notifications_delivery_rate_per_target = TimeSeries(
    title='Notification Delivery Rate',
    description='Number of delivered notifications in a given time',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum by(target) (rate(' + Metrics.DELIVERY_LAG.count(status="success") + '))',
        legendFormat='{{target}}',
    )])

notifications_failure_rate_per_target = TimeSeries(
    title='Failed Notifications Per Target In Time',
    description='Number of failed notifications in a given time',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum by(target) (rate(' + Metrics.DELIVERY_LAG.count(status="failure") + '))',
        legendFormat='{{target}}',
    )])

event_types_processed_rate = TimeSeries(
    title='Event Types Processed',
    description='Number of notification events of a given type that where processed',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum by(eventType) (rate(' + Metrics.PROCESSOR_EVENTS() + '))',
        legendFormat='{{eventType}}',
    )])

processed_events_in_time = TimeSeries(
    title='Total Processed Oplog Events',
    description='Number of processed oplog events by the notification producer',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='hidden',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum(rate(' + Metrics.CONSUMED_EVENTS() + '))',
        legendFormat='{{target}}',
    )])

bandwith_rate_per_target = TimeSeries(
    title='Bandwidth',
    description='Bandwidth used to send notifications to targets in a given time',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.BYTES_SEC,
    targets=[Target(
        expr='sum by(target) (rate(' + Metrics.NOTIF_SIZE() + '))',
        legendFormat='{{target}}',
    )])

cache_get_lag = TimeSeries(
    title='Cache Get Lag',
    description='Time it takes to get a bucket notification config from MongoDB',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'sum by(job) (rate(' + Metrics.CACHE_LAG.sum(cache_hit="true") + '))',
            ', "job", "$1", "job", "' + LABEL_EXPR['notification-processors'] +'")',
        ]),
        legendFormat='{{target}}',
    )])

notification_delivery_delay = Heatmap(
    title='Delivery Delay',
    description='Delay between when a notification is sent and when it is received',
    dataSource='${DS_PROMETHEUS}',
    hideZeroBuckets=True,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SECONDS, decimals=0),
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='sum(rate(' + Metrics.DELIVERY_LAG.sum(status="success") + ')) by(le)',
        legendFormat='{{le}}',
    )])

cached_buckets = TimeSeries(
    title='Cached Notification Configurations',
    description='Total number of notification configurations cached',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'sum(' + Metrics.CACHED_BUCKETS.raw()  + ') by (job)',
            ', "job", "$1", "job", "' + LABEL_EXPR['notification-processors'] +'")',
        ]),
        legendFormat='{{job}}',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

cache_updates = TimeSeries(
    title='Cache Updates',
    description='Rate of cache updates',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    legendDisplayMode='table',
    legendPlacement='right',
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'sum by (job) (rate (' + Metrics.CACHE_UPDATES() + '))',
            ', "job", "$1", "job", "' + LABEL_EXPR['notification-processors'] +'")',
        ]),
        legendFormat='{{job}}',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

cache_hit_miss_ratio = GaugePanel(
    title='Cache Hit/Miss Ratio',
    description='Cache Hit/Miss Ratio',
    dataSource='${DS_PROMETHEUS}',
    noValue='0',
    min='0',
    max='1',
    format=UNITS.PERCENT_UNIT,
    targets=[Target(
        expr='\n'.join([
            'label_replace(',
            'sum by (job) (rate (' + Metrics.CACHE_LAG.count(cache_hit="true") + '))',
            '/',
            'sum by (job) (rate (' + Metrics.CACHE_LAG.count() +'))',
            ', "job", "$1", "job", "' + LABEL_EXPR['notification-processors'] +'")',
        ]),
        legendFormat='{{job}}',
    )],
    thresholds=[
        Threshold("red",     1, 0.00),
        Threshold("orange",  2, 0.85),
        Threshold("green",   3, 0.90),
    ])

dashboard = (
    Dashboard(
        title='Bucket Notifications',
        editable=True,
        refresh='30s',
        tags=['backbeat', 'notification'],
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
                name='job_notification_producer',
                label='notification producer',
                description='Name of the notification producer job to filter metrics',
                value='artesca-data-backbeat-notification-producer-headless',
            ),
            ConstantInput(
                name='job_notification_processors',
                label='notification processors',
                description='Name of the notification processor jobs to filter metrics',
                value='artesca-data-backbeat-notification-processor',
            ),
            ConstantInput(
                name='job_zookeeper',
                label='zookeeper quorom',
                description='Name of the zookeeper jobs to filter metrics',
                value='artesca-data-base-quorum-headless',
            ),
            ConstantInput(
                name='job_kafka',
                label='kafka brokers',
                description='Name of the kafka jobs to filter metrics',
                value='artesca-data-base-queue',
            ),
            ConstantInput(
                name='notification_topics',
                label='bucket notification topics',
                description='Name of the bucket notification kafka topics',
                value='.*backbeat-notification.*',
            ),
        ],
        templating=Templating([
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Notification processor',
                name='notification_processor',
                query='label_values(' + Metrics.PROCESSOR_EVENTS.raw('job=~"${job_notification_processors}.*"') + ', job)',
                regex='/^(?<value>(' + DEFAULT_JOB_PREFIX + ')?(backbeat-notification-processor-)?(?<text>.*?)(-headless)?)$/',
                allValue='${job_notification_processors}.*',
                includeAll=True,
                multi=True,
            ),
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Notification target',
                name='target',
                query='label_values(' + Metrics.PROCESSOR_EVENTS.raw('job=~"${job_notification_processors}.*"', 'target=~".*"') + ', target)',
                allValue='.*',
                includeAll=True,
                multi=True,
            ),
        ]),
        panels=layout.column([
            RowPanel(title='Service Status'),
            layout.row([
                zookeeper_quorum,
                kafka_brokers,
                producer_up_current,
                success_rate,
                notification_rate,
                bandwidth_rate,
                ], height=3),
            layout.row([
                processors_up_current,
                kafka_lag,
                ], height=8),
            RowPanel(title='Notification Metrics'),
            layout.row([
                notifications_delivery_rate_per_target,
                notifications_failure_rate_per_target,
            ], height=7),
            layout.row([
                event_types_processed_rate,
                processed_events_in_time,
            ], height=7),
            layout.row([
                bandwith_rate_per_target,
                notification_delivery_delay,
            ], height=7),
            RowPanel(title='Cache Metrics'),
            layout.row([
                cache_get_lag,
                cache_updates,
            ], height=7),
            layout.row([
                cached_buckets,
                cache_hit_miss_ratio,
            ], height=7),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
