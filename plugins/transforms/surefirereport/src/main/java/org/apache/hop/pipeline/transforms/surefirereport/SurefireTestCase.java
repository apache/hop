/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.surefirereport;

/** One Surefire testcase row accumulated by {@link SurefireReportOutput}. */
public class SurefireTestCase {
  public enum Status {
    PASS,
    FAIL,
    ERROR,
    SKIP
  }

  private final String name;
  private final double durationSeconds;
  private final Status status;
  private final String systemOut;
  private final String systemErr;
  private final String failureMessage;
  private final String failureType;

  public SurefireTestCase(
      String name,
      double durationSeconds,
      Status status,
      String systemOut,
      String systemErr,
      String failureMessage,
      String failureType) {
    this.name = name;
    this.durationSeconds = durationSeconds;
    this.status = status;
    this.systemOut = systemOut;
    this.systemErr = systemErr;
    this.failureMessage = failureMessage;
    this.failureType = failureType;
  }

  public String getName() {
    return name;
  }

  public double getDurationSeconds() {
    return durationSeconds;
  }

  public Status getStatus() {
    return status;
  }

  public String getSystemOut() {
    return systemOut;
  }

  public String getSystemErr() {
    return systemErr;
  }

  public String getFailureMessage() {
    return failureMessage;
  }

  public String getFailureType() {
    return failureType;
  }
}
