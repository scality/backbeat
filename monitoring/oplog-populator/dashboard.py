from grafanalib.core import (
    ConstantInput,
    DataSourceInput,
    Threshold,
    Templating,
    Template,
    Heatmap,
    YAxis,
    HeatmapColor,
    RowPanel,
)
from grafanalib import formatunits as UNITS
from scalgrafanalib import(
    metrics,
    layout,Target,
    TimeSeries,
    Dashboard,
    Stat,
    PieChart,
    Tooltip,
)

class Metrics:
    RECONFIG_LAG = metrics.BucketMetric(
        's3_oplog_populator_reconfiguration_lag_seconds', 'connector',
        job="${oplog_populator_job}", namespace="${namespace}"
    ).with_defaults(
        'connector=~"${connector}"',
    )

    ACK_LAG = metrics.BucketMetric(
        's3_oplog_populator_acknowledgement_lag_seconds',
        'opType', 'job', namespace="${namespace}"
    ).with_defaults(
        'job="${oplog_populator_job}"',
    )

    CONNECTOR_CONFIG, CONNECTOR_CONFIG_APPLIED, BUCKETS, REQUEST_SIZE, PIPELINE_SIZE = [
        metrics.CounterMetric(
            name, 'connector', *extraLabels,
            job="${oplog_populator_job}", namespace="${namespace}"
        ).with_defaults(
            'connector=~"${connector}"',
        ) for name, extraLabels in {
            's3_oplog_populator_connector_configurations_total': ['opType'],
            's3_oplog_populator_connector_configuration_applied_total': ['success'],
            's3_oplog_populator_connector_buckets': [],
            's3_oplog_populator_connector_request_bytes_total': [],
            's3_oplog_populator_connector_pipeline_bytes': [],
        }.items()
    ]

    CONNECTORS, MIGRATIONS = [
        metrics.CounterMetric(
            name, label,
            job="${oplog_populator_job}", namespace="${namespace}"
        ) for name, label in {
            's3_oplog_populator_connectors': 'state',
            's3_oplog_populator_migrations_total': 'success',
        }.items()
    ]

    MONGODB_LAG = metrics.CounterMetric(
        'kafka_connect_mongodb_source_task_metrics_latest_mongodb_time_difference_secs',
        'connector', 'task',
        job="${kafka_connect_job}", namespace="${namespace}"
    ).with_defaults(
        'connector=~"${connector}"',
    )

oplog_populator_up_current = Stat(
    title='Oplog Populator Instances',
    description='Number of active oplog populator instances',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='-',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'sum(up{',
            '    namespace="${namespace}",',
            '    job="${oplog_populator_job}"',
            '}) or vector(0)',
        ]),
    )],
    thresholds=[
        Threshold('red', 0, 0.),
        Threshold('green', 1, 1.),
    ])

kafka_connect_up_current = Stat(
    title='Kafka Connect Instances',
    description='Number of active kafka-connect instances',
    dataSource='${DS_PROMETHEUS}',
    colorMode='background',
    noValue='-',
    reduceCalc='last',
    targets=[Target(
        expr='\n'.join([
            'sum(up{',
            '    namespace="${namespace}",',
            '    job="${kafka_connect_job}"',
            '}) or vector(0)',
        ]),
    )],
    thresholds=[
        Threshold('red', 0, 0.),
        Threshold('green', 1, 1.),
    ])

connector_instances = Stat(
    title='Active Connectors',
    description='Number of active kafka-connect connector instances',
    dataSource='${DS_PROMETHEUS}',
    colorMode='value',
    noValue='-',
    reduceCalc='last',
    targets=[Target(
        expr='sum(' + Metrics.CONNECTORS.raw('state=~"|running"') + ')',
    )],
    thresholds=[
        Threshold('red', 0, 0.),
        Threshold('green', 1, 1.),
    ])

total_number_buckets = Stat(
    title='Monitored Buckets',
    description='Number of buckets configured on the connectors',
    dataSource='${DS_PROMETHEUS}',
    colorMode='value',
    noValue='-',
    reduceCalc='last',
    targets=[Target(
        expr='sum(' + Metrics.BUCKETS.raw() + ')',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

max_pipeline_size = Stat(
    title='Max Pipeline Size',
    description='Maximum size of the mongo pipeline',
    dataSource='${DS_PROMETHEUS}',
    colorMode='value',
    noValue='-',
    reduceCalc='last',
    format=UNITS.BYTES,
    orientation='horizontal',
    targets=[Target(
        expr='max (' + Metrics.PIPELINE_SIZE.raw() + ')',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

bandwidth = Stat(
    title='Bandwidth',
    description='Bandwidth used for reconfiguring the connectors',
    dataSource='${DS_PROMETHEUS}',
    colorMode='value',
    noValue='-',
    reduceCalc='last',
    format=UNITS.BYTES_SEC,
    orientation='horizontal',
    targets=[Target(
        expr='sum (rate(' + Metrics.REQUEST_SIZE() + ')) or vector(0)',
    )],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

config_applied_success = TimeSeries(
    title='Connector Reconfiguration Rate',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    legendDisplayMode='hidden',
    fillOpacity=10,
    spanNulls=True,
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG_APPLIED('success="true"') + ')) or vector(0)',
    )])

config_applied_failure = TimeSeries(
    title='Failed Connector Reconfiguration Rate',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    legendDisplayMode='hidden',
    fillOpacity=10,
    spanNulls=True,
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG_APPLIED('success="false"') + ')) or vector(0)',
    )])


bucket_distribution = PieChart(
    title='Bucket Distribution',
    description='How buckets are distributed among the connectors',
    dataSource='${DS_PROMETHEUS}',
    legendValues=['percent', 'value'],
    pieType='donut',
    legendDisplayMode='table',
    legendPlacement='right',
    targets=[Target(
        expr='sum by(connector) (' + Metrics.BUCKETS.raw() + ')',
        legendFormat='{{connector}}',
    )])

bucket_per_connector = TimeSeries(
    title='Buckets Per Connector',
    description='Evolution of buckets per connector',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=True,
    targets=[Target(
        expr='sum by(connector) (' + Metrics.BUCKETS.raw() + ')',
        legendFormat='{{connector}}',
    )])

bucket_additions = TimeSeries(
    title='Bucket Addition Rate',
    description='Rate at which buckets are getting added to the connectors',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    legendDisplayMode='hidden',
    fillOpacity=10,
    spanNulls=True,
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG(opType="add") + '))',
    )])

bucket_removals = TimeSeries(
    title='Bucket Removal Rate',
    description='Rate at which buckets are getting removed from the connectors',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    legendDisplayMode='hidden',
    fillOpacity=10,
    spanNulls=True,
    unit=UNITS.OPS_PER_SEC,
    targets=[Target(
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG(opType="delete") + '))',
    )])


ack_lag = Heatmap(
    title='Acknowledgement Lag',
    description='Delay between a config change in mongo and the start of processing by the oplogPopulator in seconds',
    dataSource='${DS_PROMETHEUS}',
    dataFormat='tsbuckets',
    hideZeroBuckets=True,
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SECONDS),
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='\n'.join([
            'sum(rate(',
            '  ' + Metrics.ACK_LAG.bucket(),
            ')) by(le)',
        ]),
        format="heatmap",
        legendFormat="{{le}}",
    )],
)

reconfig_lag = Heatmap(
    title='Reconfig Lag',
    description='Time it takes kafka-connect to respond to a connector configuration request',
    dataSource='${DS_PROMETHEUS}',
    dataFormat='tsbuckets',
    hideZeroBuckets=True,
    maxDataPoints=25,
    tooltip=Tooltip(show=True, showHistogram=True),
    yAxis=YAxis(format=UNITS.SECONDS),
    cards={'cardPadding': 1, 'cardRound': 2},
    color=HeatmapColor(mode='opacity'),
    targets=[Target(
        expr='\n'.join([
            'sum(rate(',
            '  ' + Metrics.RECONFIG_LAG.bucket(),
            ')) by(le)',
        ]),
        format="heatmap",
        legendFormat="{{le}}",
    )],
)

hashed_connectors_by_state = TimeSeries(
    title='Hashed Connectors',
    description='Connectors by state with hashed ingestion: running, paused (or stopped) and failed hashed '
                'connectors, and outdated connectors waiting to be migrated',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='stepAfter',
    fillOpacity=10,
    spanNulls=False,
    targets=[Target(
        expr='sum by(state) (' + Metrics.CONNECTORS.raw('state!=""') + ')',
        legendFormat='{{state}}',
    )])

migrations = Stat(
    title='Migrations',
    description='Migrations to hashed connectors over the selected time range, by outcome',
    dataSource='${DS_PROMETHEUS}',
    colorMode='value',
    noValue='-',
    reduceCalc='last',
    targets=[
        Target(
            expr='round(sum(increase(' + Metrics.MIGRATIONS.raw('success="true"') + '[$__range])))',
            legendFormat='Completed',
        ),
        Target(
            expr='round(sum(increase(' + Metrics.MIGRATIONS.raw('success="false"') + '[$__range])))',
            legendFormat='Failed',
        ),
    ],
    thresholds=[
        Threshold('green', 0, 0.),
    ])

mongodb_lag = TimeSeries(
    title='Oplog Lag Per Connector',
    description='Delay between the latest oplog entry and the last one read by each connector',
    dataSource='${DS_PROMETHEUS}',
    gradientMode='opacity',
    lineInterpolation='smooth',
    fillOpacity=10,
    spanNulls=False,
    unit=UNITS.SECONDS,
    targets=[Target(
        expr='max by(connector) (' + Metrics.MONGODB_LAG.raw() + ')',
        legendFormat='{{connector}}',
    )])


dashboard = (
    Dashboard(
        title='Oplog Populator',
        editable=True,
        refresh='30s',
        tags=['backbeat', 'oplogPopulator'],
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
                name='oplog_populator_job',
                label='oplog populator job',
                description='Name of the oplog populator job to filter metrics',
                value='artesca-data-backbeat-oplog-populator-headless',
            ),
            ConstantInput(
                name='kafka_connect_job',
                label='kafka connect job',
                description='Name of the kafka connect job to filter metrics',
                value='artesca-data-base-queue-connector-metrics',
            ),
        ],
        templating=Templating([
            Template(
                dataSource='${DS_PROMETHEUS}',
                label='Connector',
                name='connector',
                # hashed connectors record their reconfigurations under a
                # single label, which only the configuration metrics carry
                query='label_values({__name__=~"' + '|'.join([
                    Metrics.PIPELINE_SIZE.name,
                    Metrics.CONNECTOR_CONFIG_APPLIED.name,
                ]) + '", job=~"${oplog_populator_job}.*", namespace="${namespace}"}, connector)',
                allValue='.*',
                includeAll=True,
                multi=True,
            ),
        ]),
        panels=layout.column([
            RowPanel(title="Overview"),
            layout.row([
                *layout.resize([
                    oplog_populator_up_current,
                    kafka_connect_up_current,
                    connector_instances,
                    max_pipeline_size,
                ], width=5),
                bandwidth,
            ], 3),
            layout.row([
                mongodb_lag,
            ], 7),
            layout.row([
                config_applied_success,
                config_applied_failure,
            ], 7),
            layout.row([
                reconfig_lag,
            ], 7),
            RowPanel(title="Hashed Ingestion"),
            layout.row([
                hashed_connectors_by_state,
                migrations,
            ], 7),
            RowPanel(title="Per-bucket Ingestion", collapsed=True, panels=layout.column([
                layout.row([
                    total_number_buckets,
                ], 3),
                layout.row([
                    bucket_distribution,
                    bucket_per_connector,
                ], 7),
                layout.row([
                    bucket_additions,
                    bucket_removals,
                ], 7),
                layout.row([
                    ack_lag,
                ], 7),
            ])),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
