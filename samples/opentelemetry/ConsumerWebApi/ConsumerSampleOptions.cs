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

namespace EventHorizon.RocketMQ.Samples.OpenTelemetry.ConsumerWebApi;

internal sealed class ConsumerSampleOptions
{
    public string ServiceName { get; set; } = "eventhorizon-rocketmq-otel-consumer";

    public string OtlpEndpoint { get; set; } = "http://127.0.0.1:4317";

    public Uri GetOtlpEndpoint()
    {
        if (!Uri.TryCreate(OtlpEndpoint, UriKind.Absolute, out var endpoint) ||
            (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps) ||
            string.IsNullOrWhiteSpace(endpoint.Host))
        {
            throw new InvalidOperationException("Sample:OtlpEndpoint must be an absolute HTTP or HTTPS URI.");
        }

        return endpoint;
    }

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ServiceName);
        _ = GetOtlpEndpoint();
    }
}
