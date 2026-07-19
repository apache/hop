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

package org.apache.hop.spark.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.Test;

class SparkRunModeTest {

  @Test
  void parseConfigValueDefaultsToDistributed() {
    assertEquals(SparkRunMode.DISTRIBUTED, SparkRunMode.parseConfigValue(null));
    assertEquals(SparkRunMode.DISTRIBUTED, SparkRunMode.parseConfigValue(""));
    assertEquals(SparkRunMode.DISTRIBUTED, SparkRunMode.parseConfigValue("DISTRIBUTED"));
    assertEquals(SparkRunMode.DRIVER_ONLY, SparkRunMode.parseConfigValue("DRIVER_ONLY"));
    assertEquals(SparkRunMode.DRIVER_ONLY, SparkRunMode.parseConfigValue("driver_only"));
  }

  @Test
  void resolveInheritsRunConfiguration() {
    TransformMeta transform = new TransformMeta();
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    assertEquals(SparkRunMode.DISTRIBUTED, SparkRunMode.resolve(transform, config));

    config.setGenericTransformRunMode(SparkRunMode.DRIVER_ONLY.name());
    assertEquals(SparkRunMode.DRIVER_ONLY, SparkRunMode.resolve(transform, config));
  }

  @Test
  void forceOverrideBeatsRunConfiguration() {
    TransformMeta transform = new TransformMeta();
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setGenericTransformRunMode(SparkRunMode.DRIVER_ONLY.name());

    SparkRunMode.setOverride(transform, SparkRunMode.OVERRIDE_FORCE_DISTRIBUTED);
    assertEquals(SparkRunMode.DISTRIBUTED, SparkRunMode.resolve(transform, config));
    assertTrue(SparkRunMode.hasForcedOverride(transform));

    SparkRunMode.setOverride(transform, SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY);
    config.setGenericTransformRunMode(SparkRunMode.DISTRIBUTED.name());
    assertEquals(SparkRunMode.DRIVER_ONLY, SparkRunMode.resolve(transform, config));
  }

  @Test
  void inheritClearsAttribute() {
    TransformMeta transform = new TransformMeta();
    SparkRunMode.setOverride(transform, SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY);
    assertEquals(
        SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY,
        transform.getAttribute(SparkRunMode.ATTRIBUTE_GROUP, SparkRunMode.ATTRIBUTE_KEY));

    SparkRunMode.setOverride(transform, SparkRunMode.OVERRIDE_INHERIT);
    assertFalse(SparkRunMode.hasForcedOverride(transform));
    assertEquals(SparkRunMode.OVERRIDE_INHERIT, SparkRunMode.getOverride(transform));
  }

  @Test
  void displayLabelRoundTrip() {
    assertEquals(
        SparkRunMode.OVERRIDE_FORCE_DISTRIBUTED,
        SparkRunMode.overrideFromDisplayLabel(SparkRunMode.DISPLAY_FORCE_DISTRIBUTED));
    assertEquals(
        SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY,
        SparkRunMode.overrideFromDisplayLabel(SparkRunMode.DISPLAY_FORCE_DRIVER_ONLY));
    assertEquals(
        SparkRunMode.OVERRIDE_INHERIT,
        SparkRunMode.overrideFromDisplayLabel(SparkRunMode.DISPLAY_INHERIT));
    assertEquals(
        SparkRunMode.DISPLAY_FORCE_DRIVER_ONLY,
        SparkRunMode.displayLabelForOverride(SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY));
  }
}
