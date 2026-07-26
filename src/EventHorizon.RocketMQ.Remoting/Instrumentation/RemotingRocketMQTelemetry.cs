// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"). You may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace EventHorizon.RocketMQ.Remoting.Instrumentation;

internal sealed class RemotingRocketMQTelemetry : IRemotingRocketMQTelemetry
{
    private const string MessagingSystem = "rocketmq";
    private readonly Counter<long>? _operationCount;
    private readonly Counter<long>? _messageCount;
    private readonly Histogram<double>? _operationDuration;
    private readonly bool _isEnabled;

    private RemotingRocketMQTelemetry()
    {
    }

    public RemotingRocketMQTelemetry(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        var meter = meterFactory.Create(new MeterOptions(RemotingRocketMQInstrumentation.MeterName));
        _operationCount = meter.CreateCounter<long>(
            "rocketmq.client.operations",
            "{operation}",
            "Number of completed RocketMQ client operations.");
        _messageCount = meter.CreateCounter<long>(
            "rocketmq.client.messages",
            "{message}",
            "Number of RocketMQ messages handled by client operations.");
        _operationDuration = meter.CreateHistogram<double>(
            "rocketmq.client.operation.duration",
            "s",
            "Duration of RocketMQ client operations.");
        _isEnabled = true;
    }

    internal static RemotingRocketMQTelemetry Disabled { get; } = new();

    public IRemotingRocketMQTelemetryOperation StartSend(string topic, int messageCount, long bodySize) =>
        Start("send", "send", ActivityKind.Producer, topic, null, messageCount, bodySize, null, null, null, null);

    public IRemotingRocketMQTelemetryOperation StartReceive(
        string topic,
        string consumerGroup,
        int messageCount,
        ActivityContext? parentContext,
        DateTimeOffset startTime,
        long startTimestamp,
        int? queueId = null,
        bool createActivity = true,
        IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties = null) =>
        Start(
            "receive",
            "receive",
            ActivityKind.Client,
            topic,
            consumerGroup,
            messageCount,
            null,
            null,
            messageProperties,
            parentContext,
            (startTime, startTimestamp),
            queueId,
            createActivity);

    public IRemotingRocketMQTelemetryOperation StartProcess(
        string topic,
        string consumerGroup,
        string messageId,
        int queueId,
        long bodySize,
        IReadOnlyDictionary<string, string> properties,
        ActivityContext? receiveContext)
    {
        var parentContext = receiveContext;
        IEnumerable<IReadOnlyDictionary<string, string>>? links = [properties];
        if (!HasValidContext(parentContext) && TryExtractContext(properties, out var propagatedContext))
        {
            parentContext = propagatedContext;
            links = null;
        }

        return Start(
            "process",
            "process",
            ActivityKind.Consumer,
            topic,
            consumerGroup,
            1,
            bodySize,
            messageId,
            links,
            parentContext,
            null,
            queueId);
    }

    public IRemotingRocketMQTelemetryOperation StartSettle(
        string operationName,
        string topic,
        string consumerGroup,
        string? messageId,
        int? queueId,
        IReadOnlyDictionary<string, string>? properties) =>
        Start(
            operationName,
            "settle",
            ActivityKind.Client,
            topic,
            consumerGroup,
            1,
            null,
            messageId,
            properties is null ? null : [properties],
            null,
            null,
            queueId);

    public void InjectContext(Activity? activity, IDictionary<string, string> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (activity is null)
        {
            return;
        }

        var propagator = DistributedContextPropagator.Current;
        if (propagator.Fields.Any(field => ContainsProperty(properties, field)))
        {
            return;
        }

        propagator.Inject(activity, properties, static (carrier, fieldName, fieldValue) =>
        {
            ((IDictionary<string, string>)carrier!)[fieldName] = fieldValue;
        });
    }

    private IRemotingRocketMQTelemetryOperation Start(
        string operationName,
        string operationType,
        ActivityKind kind,
        string topic,
        string? consumerGroup,
        int messageCount,
        long? bodySize,
        string? messageId,
        IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties,
        ActivityContext? parentContext,
        (DateTimeOffset StartTime, long StartTimestamp)? start = null,
        int? queueId = null,
        bool createActivity = true)
    {
        if (!_isEnabled)
        {
            return Operation.Disabled;
        }

        var recordOperationCount = _operationCount?.Enabled == true;
        var recordMessageCount = _messageCount?.Enabled == true;
        var recordOperationDuration = _operationDuration?.Enabled == true;
        var recordActivity = createActivity && RemotingRocketMQInstrumentation.ActivitySource.HasListeners();
        if (!recordActivity && !recordOperationCount && !recordMessageCount && !recordOperationDuration)
        {
            return Operation.Disabled;
        }

        var timing = start ?? (DateTimeOffset.UtcNow, Stopwatch.GetTimestamp());
        Activity? activity = null;
        if (recordActivity)
        {
            var tags = new ActivityTagsCollection
            {
                { "messaging.system", MessagingSystem },
                { "messaging.operation.name", operationName },
                { "messaging.operation.type", operationType },
                { "messaging.destination.name", topic },
                { "messaging.batch.message_count", messageCount }
            };
            if (!string.IsNullOrEmpty(consumerGroup))
            {
                tags.Add("messaging.consumer.group.name", consumerGroup);
            }

            if (bodySize is not null)
            {
                tags.Add("messaging.message.body.size", bodySize.Value);
            }

            if (!string.IsNullOrEmpty(messageId))
            {
                tags.Add("messaging.message.id", messageId);
            }

            if (queueId is not null)
            {
                tags.Add("messaging.destination.partition.id", queueId.Value.ToString());
            }

            var parent = parentContext ?? Activity.Current?.Context ?? default;
            activity = RemotingRocketMQInstrumentation.ActivitySource.StartActivity(
                $"{operationName} {topic}",
                kind,
                parent,
                tags,
                CreateLinks(messageProperties),
                timing.StartTime);
        }

        return new Operation(
            operationName,
            operationType,
            topic,
            messageCount,
            timing.StartTimestamp,
            activity,
            _operationCount,
            _messageCount,
            _operationDuration,
            recordOperationCount,
            recordMessageCount,
            recordOperationDuration);
    }

    private static IEnumerable<ActivityLink>? CreateLinks(
        IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties)
    {
        if (messageProperties is null)
        {
            return null;
        }

        List<ActivityLink>? links = null;
        foreach (var properties in messageProperties)
        {
            if (TryExtractContext(properties, out var context))
            {
                (links ??= []).Add(new ActivityLink(context));
            }
        }

        return links;
    }

    private static bool TryExtractContext(
        IReadOnlyDictionary<string, string> properties,
        out ActivityContext context)
    {
        DistributedContextPropagator.Current.ExtractTraceIdAndState(
            properties,
            GetPropagationField,
            out var traceId,
            out var traceState);
        return ActivityContext.TryParse(traceId, traceState, out context);
    }

    private static bool HasValidContext(ActivityContext? context) =>
        context.HasValue && context.Value.TraceId != default;

    private static void GetPropagationField(
        object? carrier,
        string fieldName,
        out string? fieldValue,
        out IEnumerable<string>? fieldValues)
    {
        fieldValues = null;
        fieldValue = carrier is IReadOnlyDictionary<string, string> properties
            ? GetProperty(properties, fieldName)
            : null;
    }

    private static bool ContainsProperty(IEnumerable<KeyValuePair<string, string>> properties, string name) =>
        properties.Any(property => string.Equals(property.Key, name, StringComparison.OrdinalIgnoreCase));

    private static string? GetProperty(IReadOnlyDictionary<string, string> properties, string name)
    {
        foreach (var property in properties)
        {
            if (string.Equals(property.Key, name, StringComparison.OrdinalIgnoreCase))
            {
                return property.Value;
            }
        }

        return null;
    }

    internal sealed class Operation : IRemotingRocketMQTelemetryOperation
    {
        private readonly string _operationName;
        private readonly string _operationType;
        private readonly string _topic;
        private readonly int _messageCount;
        private readonly long _startTimestamp;
        private readonly Activity? _activity;
        private readonly Counter<long>? _operationCount;
        private readonly Counter<long>? _messageCountMetric;
        private readonly Histogram<double>? _operationDuration;
        private readonly bool _recordOperationCount;
        private readonly bool _recordMessageCount;
        private readonly bool _recordOperationDuration;
        private bool _completed;

        private Operation()
        {
            _operationName = string.Empty;
            _operationType = string.Empty;
            _topic = string.Empty;
        }

        internal static Operation Disabled { get; } = new();

        internal Operation(
            string operationName,
            string operationType,
            string topic,
            int messageCount,
            long startTimestamp,
            Activity? activity,
            Counter<long>? operationCount,
            Counter<long>? messageCountMetric,
            Histogram<double>? operationDuration,
            bool recordOperationCount,
            bool recordMessageCount,
            bool recordOperationDuration)
        {
            _operationName = operationName;
            _operationType = operationType;
            _topic = topic;
            _messageCount = messageCount;
            _startTimestamp = startTimestamp;
            _activity = activity;
            _operationCount = operationCount;
            _messageCountMetric = messageCountMetric;
            _operationDuration = operationDuration;
            _recordOperationCount = recordOperationCount;
            _recordMessageCount = recordMessageCount;
            _recordOperationDuration = recordOperationDuration;
        }

        public Activity? Activity => _activity;

        public void Complete() => Complete(true, null, null);

        public void Complete(bool success, string? outcome = null) => Complete(success, outcome, null);

        public void Complete(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            Complete(false, null, exception);
        }

        public void Dispose()
        {
            if (!_completed)
            {
                Complete(false, "abandoned", null);
            }
        }

        private void Complete(bool success, string? outcome, Exception? exception)
        {
            if (_completed || (!_recordOperationCount &&
                               !_recordMessageCount &&
                               !_recordOperationDuration &&
                               _activity is null))
            {
                return;
            }

            _completed = true;
            if (!string.IsNullOrEmpty(outcome))
            {
                _activity?.SetTag("messaging.rocketmq.consume.result", outcome);
            }

            if (!success)
            {
                _activity?.SetStatus(ActivityStatusCode.Error, exception?.Message);
                if (exception is not null)
                {
                    _activity?.SetTag("error.type", exception.GetType().FullName);
                }
            }

            if (_recordOperationCount || _recordMessageCount || _recordOperationDuration)
            {
                var tags = new TagList
                {
                    { "messaging.system", MessagingSystem },
                    { "messaging.operation.name", _operationName },
                    { "messaging.operation.type", _operationType },
                    { "messaging.destination.name", _topic },
                    { "success", success }
                };
                if (!success)
                {
                    tags.Add("error.type", exception?.GetType().FullName ?? outcome ?? "rocketmq.operation.failed");
                }

                if (_recordOperationCount)
                {
                    _operationCount!.Add(1, tags);
                }

                if (_recordMessageCount)
                {
                    _messageCountMetric!.Add(_messageCount, tags);
                }

                if (_recordOperationDuration)
                {
                    _operationDuration!.Record(Stopwatch.GetElapsedTime(_startTimestamp).TotalSeconds, tags);
                }
            }

            _activity?.Stop();
        }
    }
}
