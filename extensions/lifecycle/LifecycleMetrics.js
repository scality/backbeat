const async = require('async');

const config = require('../../conf/Config');

const ReplicationAPI = require('../replication/ReplicationAPI');
const PromAPIClient = require('../../lib/clients/PromAPIClient');
const { prettyDuration } = require('../../lib/util/metrics');

const LATEST_PUBLISHED = 'zenko_queue_latest_published_message_timestamp';
const LATEST_CONSUMED = 'zenko_queue_latest_consumed_message_timestamp';
const QUEUED_TOTAL = 'zenko_replication_queued_total';
const QUEUED_BYTES = 'zenko_replication_queued_bytes';
const CONSUMER_LAG = 'kafka_consumergroup_lag';

const lifecycleConfig = config.extensions.lifecycle;

const bucketTasksTopic = lifecycleConfig.bucketTasksTopic;
const bucketTasksGroup = lifecycleConfig.bucketProcessor.groupId;
const objectTasksTopic = lifecycleConfig.objectTasksTopic;
const objectTasksGroup = lifecycleConfig.objectProcessor.groupId;
const dataMoverTopic = ReplicationAPI.getDataMoverTopic();

class LifecycleMetrics {
    constructor(promAPIClient) {
        this._promAPIClient = promAPIClient;
    }

    /**
     * Build a Prometheus query to retrieve the backlog age: the time
     * between the latest consumed message timestamp and the latest
     * published message timestamp, separate for all partitions and
     * consumer groups of topics used by lifecycle transitions.
     *
     * @return {string} PROMQL query
     */
    _buildConsumerBacklogAgeQuery() {
        const topicList = [
            bucketTasksTopic,
            objectTasksTopic,
            dataMoverTopic,
        ];
        const latestPub = PromAPIClient.buildMetricSelector(
            LATEST_PUBLISHED,
            { topic: topicList });
        const latestPubAgg = `max(${latestPub}) by (topic,partition)`;

        const latestCons = PromAPIClient.buildMetricSelector(
            LATEST_CONSUMED,
            { topic: topicList });
        const latestConsAgg =
              `max(${latestCons}) by (topic,partition,consumergroup)`;

        const opModifiers = 'ignoring (consumergroup) group_right';
        const backlogPerPartitionAndGroup =
              `${latestPubAgg} - ${opModifiers} ${latestConsAgg}`;

        return `max(${backlogPerPartitionAndGroup}) by (topic,consumergroup)`;
    }

    /**
     * Build a Prometheus query to retrieve the timestamps for the latest
     * messages consumed by the data mover consumer groups, separated per
     * partition and consumer group.
     *
     * @return {string} PROMQL query
     */
    _buildLatestConsumedDataMoverTimestampsQuery() {
        const latestCons = PromAPIClient.buildMetricSelector(
            LATEST_CONSUMED,
            { topic: dataMoverTopic },
            // filter group names as a sanity check for later processing
            { consumergroup: ReplicationAPI.getConsumerGroupRegexp() });
        return `max(${latestCons}) by (partition,consumergroup)`;
    }

    /**
     * Build a Prometheus query to retrieve the data mover backlog on
     * the given counter (transition count or bytes)
     *
     * @param {string} counterName - name of Prometheus counter to query
     * @param {string} partition - partition number label
     * @param {string} toLocation - target location label
     * @param {number} backlogAge - age of backlog in seconds for this
     * partition and target location
     * @return {string} PROMQL query
     */
    _buildDataMoverBacklogQuery(
        counterName, partition, toLocation, backlogAge) {
        // filter origin to only get lifecycle backlog
        const queuedCounter = PromAPIClient.buildMetricSelector(
            counterName,
            { origin: 'lifecycle', partition, toLocation });

        // compute the backlog as the amount of increase of the queued
        // counter over the time period following the latest consumed
        // message, up to now
        const queuedIncrease = `increase(${queuedCounter}[${backlogAge}s])`;
        return `sum(${queuedIncrease})`;
    }

    /**
     * Build a Prometheus query to retrieve the consumer lag value for
     * lifecycle-related topics and consumer groups
     *
     * @return {string} PROMQL query
     */
    _buildKafkaConsumerLagQuery() {
        const consumerLagGauge = PromAPIClient.buildMetricSelector(
            CONSUMER_LAG, {
                topic: [bucketTasksTopic, objectTasksTopic],
                consumergroup: [bucketTasksGroup, objectTasksGroup],
            });
        return `sum(${consumerLagGauge}) by (topic)`;
    }


    _getBacklogMetricsItem(partition, consumerGroup,
                           fromTimestamp, nowTimestamp, log, cb) {
        // number of seconds since latest consumed message on this
        // partition
        const backlogAge = Math.round(nowTimestamp - fromTimestamp);
        if (backlogAge <= 0) {
            return process.nextTick(() => cb(null, {
                count: 0,
                bytes: 0,
            }));
        }
        const toLocation =
              ReplicationAPI.consumerGroupToLocation(consumerGroup);

        // execute two queries in parallel to get both the item count
        // and number of bytes in backlog
        return async.mapValues({
            count: QUEUED_TOTAL,
            bytes: QUEUED_BYTES,
        }, (counterName, metricName, done) => this._promAPIClient.executeQuery({
            query: this._buildDataMoverBacklogQuery(
                counterName, partition, toLocation, backlogAge),
        }, log, (err, metricValues) => {
            if (err) {
                return done(err);
            }
            return done(null, metricValues.length > 0 ?
                        Math.round(metricValues[0].value[1]) : 0);
        }), cb);
    }

    _consolidateResults(consumerBacklogAge, latestConsumedTimestamps,
                        consumerLag, backlogResults, cb) {
        // initialize per-location backlog results object with
        // available locations before aggregating values
        const toLocationBacklog = {};
        const toLocations = consumerBacklogAge
              .filter(res => res.metric.topic === dataMoverTopic)
              .map(res => ReplicationAPI.consumerGroupToLocation(
                  res.metric.consumergroup))
              .concat(latestConsumedTimestamps
                      .map(res => ReplicationAPI.consumerGroupToLocation(
                          res.metric.consumergroup)));
        toLocations.forEach(toLocation => {
            toLocationBacklog[toLocation] = {
                count: 0,
                bytes: 0,
                ageSeconds: 0,
                ageApprox: prettyDuration(0),
            };
        });
        // initialize topics backlog results
        const topicBacklog = {};
        [bucketTasksTopic, objectTasksTopic].forEach(topic => {
            topicBacklog[topic] = {
                count: 0,
                ageSeconds: 0,
                ageApprox: prettyDuration(0),
            };
        });

        // set the age of backlog for lifecycle topics and for data
        // mover target locations
        consumerBacklogAge.forEach(backlogAgeRes => {
            const { topic, consumergroup } = backlogAgeRes.metric;
            let metricObj;
            if (topic === dataMoverTopic) {
                const toLocation = ReplicationAPI.consumerGroupToLocation(
                    consumergroup);
                metricObj = toLocationBacklog[toLocation];
            } else {
                metricObj = topicBacklog[topic];
            }
            metricObj.ageSeconds =
                Math.round(Math.max(backlogAgeRes.value[1], 0));
            metricObj.ageApprox = prettyDuration(metricObj.ageSeconds);
        });
        // aggregate backlog item count and bytes returned from
        // per-partition and per-consumer data mover backlog queries
        for (let i = 0; i < backlogResults.length; ++i) {
            const { consumergroup } = latestConsumedTimestamps[i].metric;
            const toLocation = ReplicationAPI.consumerGroupToLocation(
                consumergroup);
            const metricsEntry = backlogResults[i];
            toLocationBacklog[toLocation].count += metricsEntry.count;
            toLocationBacklog[toLocation].bytes += metricsEntry.bytes;
        }
        // set the backlog item count as the returned kafka consumer
        // lag for lifecycle topics (sometimes lag is reported
        // negative, hence using Math.max() to threshold to 0)
        consumerLag.forEach(lagRes => {
            const { topic } = lagRes.metric;
            topicBacklog[topic].count = Math.max(
                Number.parseInt(lagRes.value[1], 10), 0);
        });

        // consolidate and return results into a single metrics object
        const results = {
            lifecycle: {
                backlog: {
                    bucketTasks: topicBacklog[bucketTasksTopic],
                    objectTasks: topicBacklog[objectTasksTopic],
                    transitions: toLocationBacklog,
                },
            },
        };
        return cb(null, results);
    }

    /**
     * Get a set of metrics relevant to lifecycle processing (see
     * documentation for details of those metrics)
     *
     * @param {werelogs.Logger} log - logger instance
     * @param {function} cb - callback function cb(err, metrics)
     * @return {undefined}
     */
    getMetrics(log, cb) {
        // There is some complexity involved in this function so it
        // deserves some explanations. The general flow is:
        //
        // - get time gap between latest consumed and latest published
        //   message for all lifecycle-related topics, partitions and
        //   consumer groups (consumer backlog)
        //   NOTE: Ideally we would use the timestamp of the *earliest
        //   unconsumed* message rather than the *latest consumed*
        //   one, but we don't readily have this info so using the
        //   latest consumed should be a good approximation for the
        //   backlog (a side-effect is when pushing new messages to
        //   the queue, the computed backlog time span might rise high
        //   for a short time as the latest consumed message can be
        //   very old).
        //
        // - get all timestamps for the latest consumed messages
        //
        // - get consumer lag for lifecycle tasks topics, as reported
        //   by kafka metrics
        //
        // - from the latest consumed timestamps of the data mover
        //   topic, derive and execute individual queries to fetch the
        //   actual lifecycle backlog in number of entries and bytes,
        //   for each partition / location pairs exposed in the stats,
        //   by querying replication metrics filtered for lifecycle
        //   use.
        //   NOTE: We need this because each of these pairs's backlog
        //   starts at a different time, and Prometheus API does not
        //   give a way to consolidate this into a single query. With
        //   a reasonable number of locations and partitions for the
        //   data mover topic, it should not cause an issue as the
        //   individual queries should not be too complex (worst case:
        //   the query takes more time to execute).
        //
        // - From all of the above results, create consolidated
        //   metrics to return to the caller.

        const now = Date.now() / 1000;
        // execute three queries in parallel
        async.mapValues({
            consumerBacklogAge: this._buildConsumerBacklogAgeQuery(),
            latestConsumedTimestamps:
            this._buildLatestConsumedDataMoverTimestampsQuery(),
            consumerLag: this._buildKafkaConsumerLagQuery(),
        }, (promQuery, metricName, done) => this._promAPIClient.executeQuery({
            query: promQuery,
        }, log, done), (err, queriesResults) => {
            if (err) {
                return cb(err);
            }
            // execute individual backlog queries with parallelism
            const { latestConsumedTimestamps } = queriesResults;
            async.mapLimit(latestConsumedTimestamps, 10, (entry, done) => {
                // Add one minute to the latest consumed timestamp to
                // define when the backlog begins, to avoid including
                // in the query time span a residue of backlog that we
                // already consumed, it could pollute backlog results
                // otherwise with up to a minute of already processed
                // values.
                const backlogBegin = Number.parseInt(entry.value[1], 10) + 60;
                const { partition, consumergroup } = entry.metric;
                this._getBacklogMetricsItem(
                    partition, consumergroup, backlogBegin, now, log, done);
            }, (err, backlogResults) => {
                if (err) {
                    return cb(err);
                }
                // aggregate and curate results
                const {
                    consumerBacklogAge,
                    latestConsumedTimestamps,
                    consumerLag,
                } = queriesResults;
                return this._consolidateResults(
                    consumerBacklogAge, latestConsumedTimestamps, consumerLag,
                    backlogResults, cb);
            });
            return undefined;
        });
    }
}

module.exports = LifecycleMetrics;
