/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.failure;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Why something failed: the exception and its root cause. When a transform or action reported an
 * error without throwing, the cause holds its last error log line instead, and {@link #fromLog} is
 * true.
 */
@Getter
@Setter
@NoArgsConstructor
public class FailureCause {
  private String exceptionClass;

  private String message;

  private String rootCauseClass;

  private String rootCauseMessage;

  /** The stack trace, cut off at a fixed length. */
  private String stackTrace;

  /** The first SQL state found in the cause chain, if any. */
  private String sqlState;

  /** The vendor error code that goes with {@link #sqlState}. */
  private Integer vendorCode;

  /** True when this cause was read from the error log, not taken from an exception. */
  private boolean fromLog;

  /**
   * True when credentials and known secret values were masked in the message, the root cause
   * message and the stack trace.
   */
  private boolean redacted;
}
