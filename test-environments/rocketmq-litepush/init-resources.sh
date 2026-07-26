#!/bin/sh

set -eu

mqadmin=/home/rocketmq/rocketmq-5.5.0/bin/mqadmin
nameserver_address=nameserver:9876
cluster_name=DefaultCluster
broker_name=broker-litepush
standard_topic=eventhorizon-test-topic
parent_topic=eventhorizon-test-lite-parent-topic
consumer_group=eventhorizon-test-lite-push-consumer

attempt=1
while [ "$attempt" -le 60 ]; do
    cluster_list="$($mqadmin clusterList -n "$nameserver_address" 2>/dev/null || true)"
    if printf '%s\n' "$cluster_list" | grep -Fq "$broker_name"; then
        break
    fi

    echo "Waiting for $broker_name to register with NameServer ($attempt/60)..."
    sleep 1
    attempt=$((attempt + 1))
done

if [ "$attempt" -gt 60 ]; then
    echo "$broker_name did not register with NameServer in time." >&2
    exit 1
fi

$mqadmin updateTopic \
    -n "$nameserver_address" \
    -c "$cluster_name" \
    -t "$standard_topic"

$mqadmin updateTopic \
    -n "$nameserver_address" \
    -c "$cluster_name" \
    -t "$parent_topic" \
    -a +message.type=LITE

$mqadmin updateSubGroup \
    -n "$nameserver_address" \
    -c "$cluster_name" \
    -g "$consumer_group" \
    --attributes +lite.bind.topic="$parent_topic"

$mqadmin topicRoute -n "$nameserver_address" -t "$standard_topic"
$mqadmin topicRoute -n "$nameserver_address" -t "$parent_topic"

echo "Sample resources are ready: standard topic $standard_topic; Lite parent topic $parent_topic; Lite consumer group $consumer_group."
