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

package org.apache.hop.spark.run;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

class MainSparkDatabricksEnvTest {

  @Test
  @DisabledIfEnvironmentVariable(named = "DATABRICKS_RUNTIME_VERSION", matches = ".+")
  void isDatabricksEnvironmentFalseInNormalCi() {
    // When neither DATABRICKS_RUNTIME_VERSION nor a /databricks SPARK_HOME is set (typical CI /
    // laptop), detection must stay off so spark-submit still System.exit(0) on success.
    String sparkHome = System.getenv("SPARK_HOME");
    if (sparkHome != null && sparkHome.contains("/databricks")) {
      return;
    }
    assertFalse(MainSpark.isDatabricksEnvironment());
  }

  @Test
  void isTrappedExitZeroMatchesDatabricksMessage() {
    assertTrue(
        MainSpark.isTrappedExitZero(
            new SecurityException("Program attempted to exit with code 0")));
    assertFalse(
        MainSpark.isTrappedExitZero(
            new SecurityException("Program attempted to exit with code 1")));
    assertFalse(MainSpark.isTrappedExitZero(new RuntimeException("exit with code 0")));
  }
}
