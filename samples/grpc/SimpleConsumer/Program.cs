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

using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Samples.Grpc.SimpleConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");
var consumerSection = builder.Configuration.GetRequiredSection("RocketMQ:Consumer");
var sampleSection = builder.Configuration.GetRequiredSection("Sample");
var sampleOptions = new SimpleConsumerSampleOptions();
sampleSection.Bind(sampleOptions);

builder.Services
    .AddOptions<SimpleConsumerSampleOptions>()
    .Bind(sampleSection)
    .Validate(static options => !string.IsNullOrWhiteSpace(options.Filter), "Subscription filter is required.")
    .Validate(static options => options.BatchSize > 0, "Batch size must be positive.")
    .Validate(static options => options.MaxDeliveryAttempts > 0, "Maximum delivery attempts must be positive.")
    .ValidateOnStart();

// The endpoint must target a RocketMQ Proxy; this transport does not connect directly to Brokers.
// Unlike Push consumers, SimpleConsumer leaves receiving, acknowledgement, and DLQ forwarding to the hosted worker.
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcSimpleConsumer(options =>
    {
        consumerSection.Bind(options);
        options.Subscribe("eventhorizon-test-topic", new FilterExpression(sampleOptions.Filter));
    });
builder.Services.AddHostedService<SimpleConsumer>();

await builder.Build().RunAsync();
