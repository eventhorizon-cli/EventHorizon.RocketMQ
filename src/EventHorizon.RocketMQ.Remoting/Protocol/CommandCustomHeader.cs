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

using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal abstract class CommandCustomHeader
{
    private static readonly ConcurrentDictionary<Type, Func<CommandCustomHeader, Dictionary<string, object>>>
        Encoders = new();

    private static readonly MethodInfo AddMethodInfo = typeof(Dictionary<string, object>).GetMethod("Add")!;

    private static readonly ConstructorInfo ConstructorInfo =
            typeof(Dictionary<string, object>).GetConstructor(new[] { typeof(int) })!;

    public Dictionary<string, object> Encode() => Encoders.GetOrAdd(GetType(), CreateEncoder)(this);

    private static Func<CommandCustomHeader, Dictionary<string, object>> CreateEncoder(Type headerType)
    {
        var properties = headerType.GetProperties();
        var dictionary = Expression.Parameter(typeof(Dictionary<string, object>), "dictionary");
        var header = Expression.Parameter(typeof(CommandCustomHeader), "header");
        var convertedHeader = Expression.Variable(headerType, "convertedHeader");

        var body = Expression.Block(
            new[] { dictionary, convertedHeader },
            new[]
                {
                    Expression.Assign(dictionary, Expression.New(
                        ConstructorInfo,
                        Expression.Constant(properties.Length))),
                    Expression.Assign(convertedHeader, Expression.Convert(header, headerType))
                }
                .Union<Expression>(
                    properties.Select(property => Expression.Call(dictionary, AddMethodInfo,
                        Expression.Constant(ToCamelCase(property.Name)),
                        Expression.Convert(Expression.Property(convertedHeader, property), typeof(object)))))
                .Union(new[] { dictionary }).ToArray()
        );

        var lambda = Expression.Lambda<Func<CommandCustomHeader, Dictionary<string, object>>>(body, header);
        return lambda.Compile();
    }

    private static string ToCamelCase(string str) =>
        Regex.Replace(str, @"^[A-Z]", m => m.Value.ToLowerInvariant(), RegexOptions.Compiled);
}
