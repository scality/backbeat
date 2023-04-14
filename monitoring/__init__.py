from grafanalib.core import (
    GaugePanel,
    StatRangeMappings,
    Threshold,
)

from scalgrafanalib import Target


CB_LOW_RANGE_OK = 0
CB_LOW_RANGE_THROTTLED = 2
CB_LOW_RANGE_STALLED = 10
CB_HIGH_RANGE_STALLED = 20


def s3_circuit_breaker_expr(process, job):
    labelSelector = 'namespace="${namespace}"'

    if job is not None:
        labelSelector += f',job="{job}"'

    return f'avg(s3_circuit_breaker{{{labelSelector}}})'


def s3_circuit_breaker(title, process=None, job=None):
    # for some reason grafanalib wants ints as bounds of StatRangeMappings
    # so multiply by 10 and set thresholds as whole numbers
    expr = s3_circuit_breaker_expr(process, job) + '*10'

    return GaugePanel(
        title=title,
        dataSource="${DS_PROMETHEUS}",
        min=CB_LOW_RANGE_OK,
        max=CB_HIGH_RANGE_STALLED,
        valueMaps=[
            StatRangeMappings('OK', CB_LOW_RANGE_OK, CB_LOW_RANGE_THROTTLED, 'green', 0),
            StatRangeMappings('Throttled', CB_LOW_RANGE_THROTTLED, CB_LOW_RANGE_STALLED, 'orange', 1),
            StatRangeMappings('Stalled', CB_LOW_RANGE_STALLED, CB_HIGH_RANGE_STALLED, 'red', 2),
        ],
        targets=[
            Target(expr=expr),
        ],
        thresholds=[
            Threshold("green", 0, float(CB_LOW_RANGE_OK)),
            Threshold("orange", 1, float(CB_LOW_RANGE_THROTTLED)),
            Threshold("red", 2, float(CB_LOW_RANGE_STALLED)),
        ],
    )
