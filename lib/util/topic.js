function withTopicPrefix(topic) {
    if (process.env.KAFKA_TOPIC_PREFIX) {
        return process.env.KAFKA_TOPIC_PREFIX + topic;
    }
    return topic;
}

module.exports = {
    withTopicPrefix,
};
