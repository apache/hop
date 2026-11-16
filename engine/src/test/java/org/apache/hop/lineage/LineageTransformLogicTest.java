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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.api.IDeclaresTransformLogic;
import org.apache.hop.lineage.api.IDeclaresTransformLogic.FieldLogic;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class LineageTransformLogicTest {

  private static LineageContext.Builder context() {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.TRANSFORM)
        .pipelineName("orders")
        .transformName("Calculate");
  }

  private static TransformMeta declaring(List<FieldLogic> logic) {
    ITransformMeta meta =
        mock(ITransformMeta.class, withSettings().extraInterfaces(IDeclaresTransformLogic.class));
    when(((IDeclaresTransformLogic) meta).transformLogic(any())).thenReturn(logic);
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getTransform()).thenReturn(meta);
    return transformMeta;
  }

  @Test
  void theLogicOfATransformTravelsWithItsEvents() {
    List<FieldLogic> logic =
        List.of(
            new FieldLogic("total", "formula", "[price] * [qty]"),
            new FieldLogic("tax", "calculator", "MULTIPLY(total, rate)"));
    LineageContext.Builder ctx = context();

    LineageTransformLogic.put(ctx, declaring(logic), new Variables());

    assertEquals(logic, LineageTransformLogic.get(ctx.build().getAttributes()));
  }

  @Test
  void fieldsWithoutANameOrAnExpressionAreLeftOut() {
    LineageContext.Builder ctx = context();

    LineageTransformLogic.put(
        ctx,
        declaring(
            Arrays.asList(
                new FieldLogic("", "formula", "1"),
                new FieldLogic("empty", "formula", ""),
                null,
                new FieldLogic("one", "formula", "1"))),
        new Variables());

    assertEquals(
        List.of(new FieldLogic("one", "formula", "1")),
        LineageTransformLogic.get(ctx.build().getAttributes()));
  }

  @Test
  void aTransformWithoutLogicAddsNothing() {
    LineageContext.Builder ctx = context();
    TransformMeta plain = mock(TransformMeta.class);
    when(plain.getTransform()).thenReturn(mock(ITransformMeta.class));

    LineageTransformLogic.put(ctx, plain, new Variables());
    LineageTransformLogic.put(ctx, declaring(List.of()), new Variables());
    LineageTransformLogic.put(ctx, null, new Variables());

    assertNull(ctx.build().getAttributes().get(LineageTransformLogic.ATTRIBUTE));
    assertTrue(LineageTransformLogic.get(ctx.build().getAttributes()).isEmpty());
  }

  @Test
  void aTransformThatFailsToTellItsLogicAddsNothing() {
    ITransformMeta meta =
        mock(ITransformMeta.class, withSettings().extraInterfaces(IDeclaresTransformLogic.class));
    when(((IDeclaresTransformLogic) meta).transformLogic(any()))
        .thenThrow(new IllegalStateException("broken"));
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getTransform()).thenReturn(meta);
    LineageContext.Builder ctx = context();

    LineageTransformLogic.put(ctx, transformMeta, new Variables());

    assertNull(ctx.build().getAttributes().get(LineageTransformLogic.ATTRIBUTE));
  }

  @Test
  void unreadableLogicIsNone() {
    assertTrue(
        LineageTransformLogic.get(Map.of(LineageTransformLogic.ATTRIBUTE, "not json")).isEmpty());
    assertTrue(LineageTransformLogic.get(null).isEmpty());
  }
}
