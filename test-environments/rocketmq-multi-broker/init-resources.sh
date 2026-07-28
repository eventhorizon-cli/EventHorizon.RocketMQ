#!/bin/sh

set -eu

mqadmin=/home/rocketmq/rocketmq-5.5.0/bin/mqadmin
nameserver_address=nameserver:9876
standard_topic=eventhorizon-test-topic
lite_parent_topic=eventhorizon-test-lite-parent-topic
lite_consumer_group=eventhorizon-test-lite-push-consumer
queues_per_broker=3

attempt=1
while [ "$attempt" -le 60 ]; do
    cluster_list="$($mqadmin clusterList -n "$nameserver_address" 2>/dev/null || true)"
    if printf '%s\n' "$cluster_list" | grep -Fq broker-a &&
        printf '%s\n' "$cluster_list" | grep -Fq broker-b &&
        printf '%s\n' "$cluster_list" | grep -Fq broker-c; then
        break
    fi

    echo "Waiting for all Brokers to register with NameServer ($attempt/60)..."
    sleep 1
    attempt=$((attempt + 1))
done

if [ "$attempt" -gt 60 ]; then
    echo "The Brokers did not register with NameServer in time." >&2
    exit 1
fi

for broker in broker-a:10911 broker-b:10921 broker-c:10931; do
    $mqadmin updateTopic \
        -n "$nameserver_address" \
        -b "$broker" \
        -t "$standard_topic" \
        -r "$queues_per_broker" \
        -w "$queues_per_broker"

    $mqadmin updateTopic \
        -n "$nameserver_address" \
        -b "$broker" \
        -t "$lite_parent_topic" \
        -r "$queues_per_broker" \
        -w "$queues_per_broker" \
        -a +message.type=LITE

    $mqadmin updateSubGroup \
        -n "$nameserver_address" \
        -b "$broker" \
        -g "$lite_consumer_group" \
        --attributes +lite.bind.topic="$lite_parent_topic"
done

$mqadmin topicRoute -n "$nameserver_address" -t "$standard_topic"
$mqadmin topicRoute -n "$nameserver_address" -t "$lite_parent_topic"

echo "Sample resources are ready across all Brokers with $queues_per_broker queues per Broker: standard topic $standard_topic; Lite parent topic $lite_parent_topic; Lite consumer group $lite_consumer_group."
