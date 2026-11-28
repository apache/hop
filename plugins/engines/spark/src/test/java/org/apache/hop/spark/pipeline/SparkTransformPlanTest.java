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

package org.apache.hop.spark.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupingField;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.spark.pipeline.SparkTransformPlan.Kind;
import org.apache.hop.spark.util.SparkConst;
import org.apache.hop.spark.util.SparkRunMode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The plan behind the canvas badges says how a run will handle every transform. */
class SparkTransformPlanTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void aTransformWithASparkVersionIsSparkCode() {
    Map<String, SparkTransformPlan> plan = plan(pipeline(source(), dummy("out")));

    assertEquals(SparkTransformPlan.SPARK_CODE, plan.get("out"));
  }

  @Test
  void aHopTransformOnItsOwnRunsOnEveryPartition() throws Exception {
    Map<String, SparkTransformPlan> plan = plan(pipeline(source(), filter("above"), dummy("out")));

    assertEquals(new SparkTransformPlan(Kind.HOP, null), plan.get("above"));
  }

  @Test
  void neighbouringHopTransformsRunInOneChain() throws Exception {
    Map<String, SparkTransformPlan> plan =
        plan(pipeline(source(), filter("above"), filter("below"), dummy("out")));

    for (String name : new String[] {"above", "below"}) {
      assertEquals(Kind.HOP_FUSED, plan.get(name).kind());
      assertEquals("Chain: above, below.", plan.get(name).reason());
    }
  }

  @Test
  void groupByWithGroupingFieldsRunsByGroup() {
    GroupByMeta meta = new GroupByMeta();
    meta.getGroupingFields().add(new GroupingField("name"));

    SparkTransformPlan plan =
        plan(pipeline(source(), of("group", SparkConst.GROUP_BY_PLUGIN_ID, meta))).get("group");

    assertEquals(Kind.HOP, plan.kind());
    assertReason(plan, "every group on one partition");
  }

  @Test
  void groupByWithoutGroupingFieldsRunsOnOneStream() {
    SparkTransformPlan plan =
        plan(pipeline(source(), of("group", SparkConst.GROUP_BY_PLUGIN_ID, new GroupByMeta())))
            .get("group");

    assertEquals(Kind.ONE_STREAM, plan.kind());
    assertReason(plan, "Without grouping fields");
  }

  @Test
  void addSequenceWithACounterIsSparkCode() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setDefault();

    SparkTransformPlan plan =
        plan(pipeline(source(), of("sequence", SparkConst.ADD_SEQUENCE_PLUGIN_ID, meta)))
            .get("sequence");

    assertEquals(SparkTransformPlan.SPARK_CODE, plan);
  }

  @Test
  void addSequenceFromADatabaseRunsAsHop() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setDefault();
    meta.setCounterUsed(false);
    meta.setDatabaseUsed(true);

    SparkTransformPlan plan =
        plan(pipeline(source(), of("sequence", SparkConst.ADD_SEQUENCE_PLUGIN_ID, meta)))
            .get("sequence");

    assertEquals(Kind.HOP, plan.kind());
    assertReason(plan, "database sequence");
  }

  @Test
  void tableInputWithoutPartitionsRunsAsHop() {
    SparkTransformPlan plan =
        plan(pipeline(of("read", SparkConst.TABLE_INPUT_PLUGIN_ID, new TableInputMeta())))
            .get("read");

    assertEquals(Kind.HOP, plan.kind());
    assertReason(plan, "partitioned read");
  }

  @Test
  void aTransformForcedOntoTheDriverRunsThere() throws Exception {
    TransformMeta above = filter("above");
    SparkRunMode.setOverride(above, SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY);

    SparkTransformPlan plan =
        plan(pipeline(source(), above, filter("below"), dummy("out"))).get("above");

    assertEquals(Kind.DRIVER, plan.kind());
    assertEquals("Spark Run Mode of the transform: Force Driver Only.", plan.reason());
  }

  @Test
  void theRunConfigurationPutsHopTransformsOnTheDriver() throws Exception {
    SparkPipelineRunConfiguration runConfiguration = new SparkPipelineRunConfiguration();
    runConfiguration.setGenericTransformRunMode(SparkRunMode.DRIVER_ONLY.name());
    TransformMeta below = filter("below");
    SparkRunMode.setOverride(below, SparkRunMode.OVERRIDE_FORCE_DISTRIBUTED);

    Map<String, SparkTransformPlan> plan =
        HopPipelineMetaToSparkConverter.plan(
            new Variables(),
            pipeline(source(), filter("above"), below, dummy("out")),
            runConfiguration);

    assertEquals(Kind.DRIVER, plan.get("above").kind());
    assertReason(plan.get("above"), "run configuration");
    assertEquals(Kind.HOP, plan.get("below").kind());
    assertReason(plan.get("below"), "Force distributed");
    assertEquals(SparkTransformPlan.SPARK_CODE, plan.get("out"));
  }

  @Test
  void aTransformWithASparkVersionIgnoresTheRunMode() throws Exception {
    TransformMeta out = dummy("out");
    SparkRunMode.setOverride(out, SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY);

    assertEquals(SparkTransformPlan.SPARK_CODE, plan(pipeline(source(), out)).get("out"));
  }

  @Test
  void reasonsAreJoined() {
    assertNull(SparkTransformPlan.join(null, null));
    assertEquals("A.", SparkTransformPlan.join("A.", null));
    assertEquals("B.", SparkTransformPlan.join(null, "B."));
    assertEquals("A. B.", SparkTransformPlan.join("A.", "B."));
  }

  private static void assertReason(SparkTransformPlan plan, String part) {
    assertTrue(plan.reason().contains(part), plan.reason());
    assertFalse(plan.reason().startsWith("!"), plan.reason());
  }

  private static Map<String, SparkTransformPlan> plan(PipelineMeta pipelineMeta) {
    return HopPipelineMetaToSparkConverter.plan(
        new Variables(), pipelineMeta, new SparkPipelineRunConfiguration());
  }

  /** The transforms in a row, each hop from one to the next. */
  private static PipelineMeta pipeline(TransformMeta... transforms) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    for (int i = 0; i < transforms.length; i++) {
      pipelineMeta.addTransform(transforms[i]);
      if (i > 0) {
        pipelineMeta.addPipelineHop(new PipelineHopMeta(transforms[i - 1], transforms[i]));
      }
    }
    return pipelineMeta;
  }

  private static TransformMeta source() {
    InjectorMeta meta = new InjectorMeta();
    meta.getInjectorFields().add(new InjectorField("n", "Integer", "-1", "-1"));
    meta.getInjectorFields().add(new InjectorField("name", "String", "-1", "-1"));
    return of("src", "Injector", meta);
  }

  private static TransformMeta dummy(String name) {
    return of(name, SparkConst.DUMMY_PLUGIN_ID, new DummyMeta());
  }

  private static TransformMeta filter(String name) throws Exception {
    FilterRowsMeta meta = new FilterRowsMeta();
    meta.setCondition(
        new Condition(
            "n",
            Condition.Function.LARGER,
            null,
            new ValueMetaAndData(new ValueMetaInteger("constant"), 2L)));
    return of(name, "FilterRows", meta);
  }

  private static TransformMeta of(String name, String pluginId, ITransformMeta meta) {
    TransformMeta transformMeta = new TransformMeta(name, meta);
    transformMeta.setTransformPluginId(pluginId);
    return transformMeta;
  }
}
