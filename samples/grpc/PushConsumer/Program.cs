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
using EventHorizon.RocketMQ.Samples.Grpc.PushConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

const string Topic = "eventhorizon-test-topic";

var builder = Host.CreateApplicationBuilder(args);
var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");

// The endpoint must target a RocketMQ Proxy; this transport does not connect directly to Brokers.
// Push delivery uses client-initiated long polling.
// Handler results drive automatic acknowledgement, retry, or DLQ forwarding.
// ServiceLifetime.Scoped creates a fresh DI scope for each message handling attempt.
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-sample";
        options.MaxConcurrency = 4;
        options.BatchSize = 16;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddGrpcPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-all-messages";
        options.MaxConcurrency = 2;
        options.BatchSize = 8;
        options.Subscribe(Topic);
    });

await builder.Build().RunAsync();
