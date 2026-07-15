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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class MainSparkArgsTest {

  @Test
  void parsePositionalThreeArgs() throws Exception {
    MainSparkArgs args =
        MainSparkArgs.parse(new String[] {"pipe.hpl", "meta.json", "spark-cluster"});
    assertEquals("pipe.hpl", args.getPipelinePath());
    assertEquals("meta.json", args.getMetadataPath());
    assertEquals("spark-cluster", args.getRunConfigName());
    assertNull(args.getEnvironmentFile());
  }

  @Test
  void parsePositionalWithEnvFile() throws Exception {
    MainSparkArgs args =
        MainSparkArgs.parse(new String[] {"pipe.hpl", "meta.json", "spark-cluster", "env.json"});
    assertEquals("env.json", args.getEnvironmentFile());
  }

  @Test
  void parseNamedArgs() throws Exception {
    MainSparkArgs args =
        MainSparkArgs.parse(
            new String[] {
              "--HopPipelinePath=/data/p.hpl",
              "--HopMetadataPath=/data/m.json",
              "--HopRunConfigurationName=spark-native",
              "--HopConfigFile=/data/env.json"
            });
    assertEquals("/data/p.hpl", args.getPipelinePath());
    assertEquals("/data/m.json", args.getMetadataPath());
    assertEquals("spark-native", args.getRunConfigName());
    assertEquals("/data/env.json", args.getEnvironmentFile());
  }

  @Test
  void missingArgsFails() {
    HopException e1 = assertThrows(HopException.class, () -> MainSparkArgs.parse(new String[] {}));
    assertTrue(e1.getMessage().contains("No arguments"));

    HopException e2 =
        assertThrows(HopException.class, () -> MainSparkArgs.parse(new String[] {"only.hpl"}));
    assertTrue(e2.getMessage().contains("at least 3"));

    HopException e3 =
        assertThrows(
            HopException.class,
            () ->
                MainSparkArgs.parse(
                    new String[] {"--HopPipelinePath=a.hpl", "--HopMetadataPath=b.json"
                      // missing run config
                    }));
    assertTrue(e3.getMessage().contains("required"));
  }
}
