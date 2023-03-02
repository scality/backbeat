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
            's3_oplog_populator_connector_configurations_total_applied': ['success'],
            's3_oplog_populator_connector_buckets': [],
            's3_oplog_populator_connector_request_bytes_total': [],
            's3_oplog_populator_connector_pipeline_bytes': [],
        }.items()
    ]

    CONNECTORS = metrics.CounterMetric(
        's3_oplog_populator_connectors', 'isOld',
        job="${oplog_populator_job}", namespace="${namespace}"
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
            '})',
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
            '})',
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
        expr='sum(' + Metrics.CONNECTORS.raw() + ')',
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
        expr='sum (rate(' + Metrics.REQUEST_SIZE() + '))',
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
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG_APPLIED('success="true"') + '))',
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
        expr='sum (rate(' + Metrics.CONNECTOR_CONFIG_APPLIED('success="false"') + '))',
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
                query='label_values(' + Metrics.BUCKETS.raw('job=~"${oplog_populator_job}.*"', 'connector=~".*"') + ', connector)',
                allValue='.*',
                includeAll=True,
                multi=True,
            ),
        ]),
        panels=layout.column([
            RowPanel(title="Overview"),
            layout.row([
                oplog_populator_up_current,
                kafka_connect_up_current,
                connector_instances,
                total_number_buckets,
                max_pipeline_size,
                bandwidth,
            ], 3),
            layout.row([
                config_applied_success,
                config_applied_failure,
            ], 7),
            RowPanel(title="Bucket Metrics"),
            layout.row([
                bucket_distribution,
                bucket_per_connector,
            ], 7),
            layout.row([
                bucket_additions,
                bucket_removals,
            ], 7),
            RowPanel(title="Lag Metrics"),
            layout.row([
                ack_lag,
                reconfig_lag,
            ], 7),
        ]),
    )
    .auto_panel_ids()
    .verify_datasources()
)
