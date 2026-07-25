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

namespace EventHorizon.RocketMQ.Exceptions;

/// <summary>
/// Represents an error reported by a RocketMQ client transport.
/// </summary>
public class RocketMQClientException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQClientException"/> class with an error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public RocketMQClientException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQClientException"/> class with an error message and the
    /// exception that caused the error.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that caused the current exception, if one is available.</param>
    public RocketMQClientException(string message, Exception? innerException) : base(message, innerException)
    {
    }
}
