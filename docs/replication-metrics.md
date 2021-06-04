# Replication Metrics
Prometheus metrics are provided for replication through HTTP routes.

> Currently metrics are only provided for the queue populator.

## Configuration
Under `conf/config.json` you can specify the probe server settings.

When starting processes you will have to enable the probe server by setting an environment variable.
```
export CRR_METRICS_PROBE=true
```

After you start the process you can view prometheus metrics at `http://{bindAddress}:{port}/_/metrics`.

## Connecting to Prometheus
Download and get started with [Prometheus](https://prometheus.io/) by following the
official [getting started](https://prometheus.io/docs/prometheus/latest/getting_started/)
guide.

Below is a sample configuration using the default queue populator configuration values.
```
# prom.yml
global:
  scrape_interval:     15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: queue_populator
    metrics_path: /_/metrics
    static_configs:
      - targets: ['localhost:4042']
```

Start Prometheus by providing this file as the config file parameter.
`./prometheus --config.file=prom.yml`
