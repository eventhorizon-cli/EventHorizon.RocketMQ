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
using EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var remotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting");

var rocketMQ = builder.Services.AddRocketMQRemoting(remotingSection.Bind);
rocketMQ.AddRemotingLitePullConsumer(options =>
{
    options.GroupName = "remoting-sample-lite-pull-consumer";
    options.BatchSize = 32;
    options.LongPollingTimeout = TimeSpan.FromSeconds(5);
    options.InitialOffset = QueryOffsetPolicy.End;
    options.Subscribe("eventhorizon-test-topic");
});
builder.Services.AddHostedService<LitePullConsumer>();

await builder.Build().RunAsync();
