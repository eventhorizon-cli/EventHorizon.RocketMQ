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

using System.CommandLine;

namespace EventHorizon.RocketMQ.Remoting.CrossProcessTestHost;

internal static class CrossProcessHostCommand
{
    public static Task<int> InvokeAsync(
        string[] args,
        Func<CrossProcessHostOptions, CancellationToken, Task<int>> action)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(action);

        var memberOption = CreateRequiredStringOption("--member", "Test member name.");
        var nameServerOption = CreateRequiredStringOption("--nameserver", "RocketMQ NameServer address.");
        var instanceOption = CreateRequiredStringOption("--instance", "Unique RocketMQ client instance name.");
        var groupOption = CreateRequiredStringOption("--group", "Consumer group name.");
        var topicOption = CreateRequiredStringOption("--topic", "Subscribed topic.");
        var tagOption = CreateRequiredStringOption("--tag", "Subscribed tag expression.");
        var orderlyOption = new Option<bool>("--orderly")
        {
            Description = "Whether to consume in orderly mode.",
            Required = true,
            Arity = ArgumentArity.ExactlyOne
        };
        var command = new RootCommand("Runs one member of a cross-process RocketMQ Rebalance test.");
        command.Options.Add(memberOption);
        command.Options.Add(nameServerOption);
        command.Options.Add(instanceOption);
        command.Options.Add(groupOption);
        command.Options.Add(topicOption);
        command.Options.Add(tagOption);
        command.Options.Add(orderlyOption);
        command.SetAction((parseResult, cancellationToken) => action(
            new CrossProcessHostOptions(
                GetRequiredValue(parseResult, memberOption),
                GetRequiredValue(parseResult, nameServerOption),
                GetRequiredValue(parseResult, instanceOption),
                GetRequiredValue(parseResult, groupOption),
                GetRequiredValue(parseResult, topicOption),
                GetRequiredValue(parseResult, tagOption),
                parseResult.GetValue(orderlyOption)),
            cancellationToken));
        return command.Parse(args).InvokeAsync();
    }

    private static Option<string> CreateRequiredStringOption(string name, string description)
    {
        var option = new Option<string>(name)
        {
            Description = description,
            Required = true,
            Arity = ArgumentArity.ExactlyOne
        };
        option.Validators.Add(result =>
        {
            if (string.IsNullOrWhiteSpace(result.GetValueOrDefault<string>()))
            {
                result.AddError($"Option '{name}' requires a non-empty value.");
            }
        });
        return option;
    }

    private static string GetRequiredValue(ParseResult parseResult, Option<string> option) =>
        parseResult.GetValue(option) ??
        throw new InvalidOperationException($"Required option '{option.Name}' was not parsed.");
}
