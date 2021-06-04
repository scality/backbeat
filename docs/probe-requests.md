# Probe Requests
Backbeat services include an HTTP endpoint to probe the current status of the process.
This endpoint can be used for healthchecks in tools like HAProxy or liveness checks
in Kubernetes.
It can also be manually viewed for verbose logs if needed.

> Currently the probe is only in Queue Populator and Queue Processor.

## Configuration
Under `conf/config.json` you can specify the probe server settings.

> Currently SSL/TLS is not supported

## Usage
After you start the process you can view the current status at
`http://{bindAddress}:{port}/_/live`.

An example Kubernetes probe configuration is as follows:
```
# pod.yaml
    livenessProbe:
      httpGet:
        path: /_/live
        port: ${port}
      initialDelaySeconds: 3
      periodSeconds: 3
```
