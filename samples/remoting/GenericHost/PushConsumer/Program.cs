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

using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Samples.Remoting.PushConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

const string Topic = "eventhorizon-test-topic";

var builder = Host.CreateApplicationBuilder(
    new HostApplicationBuilderSettings
    {
        Args = args,
        ContentRootPath = AppContext.BaseDirectory
    });
// NamesrvAddr discovers routes; this client then long-polls the assigned Brokers directly.
var remotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting");
var allMessagesQueueAssignmentMode = builder.Configuration.GetValue(
    "Sample:AllMessagesQueueAssignmentMode",
    RemotingPushQueueAssignmentMode.Client);

// A scoped handler is resolved in a new async DI scope for each message delivery.
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-sample-push-consumer";
        // Fresh groups start at End. Records published before first offset resolution are existing backlog;
        // use Beginning or Timestamp for replay, or an application readiness handshake for a precise cutover.
        options.InitialPosition = ConsumeFromPosition.End;
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
        options.MaxConcurrency = 4;
        options.PullBatchSize = 32;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddRemotingPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-all-messages-push-consumer";
        options.InitialPosition = ConsumeFromPosition.End;
        options.QueueAssignmentMode = allMessagesQueueAssignmentMode;
        options.MaxConcurrency = 2;
        options.PullBatchSize = 8;
        options.ConsumeMessageBatchSize = 1;
        options.Subscribe(Topic);
    });

// RunAsync starts and stops both Push roles through their SDK hosted services.
// Do not manually call IRemotingPushConsumer.StartAsync or StopAsync from this Host application.
await builder.Build().RunAsync();
