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

using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class LegacyAclSignerTests
{
    [Fact]
    public void CalculateSignature_ApacheClassicGoVector_Matches()
    {
        var signature = LegacyAclSigner.CalculateSignature(
            "Hello RocketMQ Client ACL Feature"u8,
            "adiaushdiaushd"u8);

        Assert.Equal("tAb/54Rwwcq+pbH8Loi7FWX4QSQ=", signature);
    }

    [Fact]
    public void Sign_FieldsAndBody_Canonicalizes()
    {
        var signer = new LegacyAclSigner(Options.Create(new RemotingClientOptions
        {
            AccessKey = "access",
            AccessSecret = "secret",
            SecurityToken = "token"
        }));
        var command = new RemotingCommand
        {
            ExtFields = new Dictionary<string, object>
            {
                ["z"] = true,
                ["a"] = 42
            },
            Body = "body"u8.ToArray()
        };

        signer.Sign(command);

        Assert.Equal("access", command.ExtFields["AccessKey"]);
        Assert.Equal("token", command.ExtFields["SecurityToken"]);
        Assert.Equal(
            LegacyAclSigner.CalculateSignature("access42truebody"u8, "secret"u8),
            command.ExtFields["Signature"]);
    }

    [Fact]
    public void Sign_PreviousAuthenticationFields_Replaces()
    {
        var signer = new LegacyAclSigner(Options.Create(new RemotingClientOptions
        {
            AccessKey = "access",
            AccessSecret = "secret"
        }));
        var command = new RemotingCommand
        {
            ExtFields = new Dictionary<string, object>
            {
                ["Signature"] = "old",
                ["AccessKey"] = "old",
                ["SecurityToken"] = "old",
                ["topic"] = "orders"
            }
        };

        signer.Sign(command);
        var first = Assert.IsType<string>(command.ExtFields["Signature"]);
        signer.Sign(command);

        Assert.Equal(first, command.ExtFields["Signature"]);
        Assert.Equal("access", command.ExtFields["AccessKey"]);
        Assert.False(command.ExtFields.ContainsKey("SecurityToken"));
    }

    [Fact]
    public void Constructor_PartialCredentials_Rejects()
    {
        Assert.Throws<ArgumentException>(() => new LegacyAclSigner(Options.Create(new RemotingClientOptions
        {
            AccessKey = "access"
        })));
    }
}
