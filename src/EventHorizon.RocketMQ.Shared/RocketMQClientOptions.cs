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

namespace EventHorizon.RocketMQ;

/// <summary>
/// Provides protocol-neutral configuration shared by RocketMQ clients.
/// </summary>
public abstract class RocketMQClientOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQClientOptions"/> class.
    /// </summary>
    protected RocketMQClientOptions()
    {
    }

    /// <summary>
    /// Gets or sets the base timeout for an individual request to RocketMQ.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the access key used for access-control authentication.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Gets or sets the access secret used for access-control authentication.
    /// </summary>
    public string? AccessSecret { get; set; }

    /// <summary>
    /// Gets or sets the temporary security token included with authenticated requests.
    /// </summary>
    public string? SecurityToken { get; set; }

    /// <summary>
    /// Gets or sets the client IP address used to construct the RocketMQ client identifier.
    /// </summary>
    public string ClientIP { get; set; } = "127.0.0.1";

    /// <summary>
    /// Gets or sets the instance name used to distinguish this client process.
    /// </summary>
    public string InstanceName { get; set; } = $"{Environment.ProcessId}-{Guid.NewGuid():N}";

    /// <summary>
    /// Gets or sets the resource namespace applied to RocketMQ topics and consumer groups.
    /// </summary>
    public string? Namespace { get; set; }

    /// <summary>
    /// Gets or sets whether client transport connections use TLS.
    /// </summary>
    public bool UseTLS { get; set; }

    /// <summary>
    /// Builds the RocketMQ client identifier from the configured client IP address and instance name.
    /// </summary>
    /// <returns>The client identifier.</returns>
    protected string BuildClientId() => $"{ClientIP}@{InstanceName}";

    /// <summary>
    /// Creates a shallow copy of these options for a logical client role.
    /// </summary>
    /// <param name="logicalClient">The logical client role appended to the instance name.</param>
    /// <returns>A copy whose instance name uniquely identifies the logical client.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="logicalClient"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="logicalClient"/> is empty or consists only of white-space characters.</exception>
    protected RocketMQClientOptions CloneForLogicalClient(string logicalClient)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalClient);
        var clone = (RocketMQClientOptions)MemberwiseClone();
        clone.InstanceName = $"{InstanceName}@{logicalClient}";
        return clone;
    }
}
