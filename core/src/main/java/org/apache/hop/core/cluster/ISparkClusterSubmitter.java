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
package org.apache.hop.core.cluster;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;

/**
 * Submits a {@code MainSpark} driver to a cluster in cluster deploy mode, and follows and kills it
 * by the id the cluster gave it.
 */
public interface ISparkClusterSubmitter extends AutoCloseable {

  /**
   * @param appResource URL of the fat jar, readable by the cluster
   * @return the id of the submitted driver or application
   */
  String submit(
      String appResource,
      String mainClass,
      List<String> appArgs,
      Map<String, String> sparkProperties,
      Map<String, String> environmentVariables)
      throws HopException;

  SparkDriverStatus status(String id) throws HopException;

  /**
   * @return the cluster's answer, for the log
   */
  String kill(String id) throws HopException;

  /**
   * Abort a {@link #submit} that is still running, called from another thread when the run is
   * stopped before the cluster gave the driver an id. The submit then fails; a submitter that may
   * have created the driver already removes it. A submit that returns an id anyway gets a {@link
   * #kill}.
   */
  default void cancelSubmission() {
    // the submit is a single short call
  }

  /** Where submissions go, for the log. */
  String describe();

  /** Let go of client connections once the run is followed to its end. */
  @Override
  default void close() {
    // nothing to release
  }
}
