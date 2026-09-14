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

package org.apache.hop.naming.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.naming.engine.NamingSchemeValidator.Finding;
import org.apache.hop.naming.engine.NamingSchemeValidator.Severity;
import org.apache.hop.naming.metadata.NamingCaseStyle;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeType;
import org.apache.hop.naming.metadata.NamingWordSeparator;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class NamingSchemeValidatorTest {

  @Test
  void skipVariablesAndEmpty() {
    NamingScheme scheme = snake("fields");
    assertTrue(NamingSchemeValidator.validate("${VAR}", "hop-field", List.of(scheme)).isEmpty());
    assertTrue(NamingSchemeValidator.validate("", "hop-field", List.of(scheme)).isEmpty());
  }

  @Test
  void warningWhenNoScheme() {
    List<Finding> findings = NamingSchemeValidator.validate("Order ID", "hop-field", List.of());
    assertEquals(1, findings.size());
    assertEquals(Severity.WARNING, findings.get(0).getSeverity());
  }

  @Test
  void errorWhenNameWouldChange() {
    NamingScheme scheme = snake("fields");
    List<Finding> findings =
        NamingSchemeValidator.validate("Order ID", "hop-field", List.of(scheme));
    assertEquals(1, findings.size());
    assertEquals(Severity.ERROR, findings.get(0).getSeverity());
    assertEquals("order_id", findings.get(0).getExpected());
  }

  @Test
  void okWhenAlreadyNormalized() {
    NamingScheme scheme = snake("fields");
    assertTrue(NamingSchemeValidator.validate("order_id", "hop-field", List.of(scheme)).isEmpty());
  }

  @Test
  void walkerFindsTransformName() {
    NamingScheme scheme = snake("fields");
    scheme.setType(NamingSchemeType.HOP_TRANSFORM.getCode());
    TransformMeta transform = new TransformMeta();
    transform.setName("Read Customers");
    List<Finding> findings = NamingSchemeWalker.walk(transform, "test.hpl", List.of(scheme), null);
    assertEquals(1, findings.size());
    assertEquals("Read Customers", findings.get(0).getActual());
    assertEquals("read_customers", findings.get(0).getExpected());
    assertEquals("name", findings.get(0).getFieldPath());
  }

  @Test
  void walkerDoesNotDoubleCountTransformName() {
    NamingScheme scheme = snake("fields");
    scheme.setType(NamingSchemeType.HOP_TRANSFORM.getCode());
    TransformMeta transform = new TransformMeta();
    transform.setName("Read Customers");
    List<Finding> findings = NamingSchemeWalker.walk(transform, "test.hpl", List.of(scheme), null);
    assertEquals(1, findings.size());
  }

  @Test
  void generalSchemeValidatesPluginKind() {
    NamingScheme general = snake("all");
    general.setType(NamingSchemeType.GENERAL.getCode());
    List<Finding> findings =
        NamingSchemeValidator.validate("Read Customers", "dv-hub", List.of(general));
    assertEquals(1, findings.size());
    assertEquals(Severity.ERROR, findings.get(0).getSeverity());
    assertEquals("read_customers", findings.get(0).getExpected());
  }

  @Test
  void multipleSchemesFailIfAnyWouldRewrite() {
    NamingScheme snake = snake("snake");
    NamingScheme dash = snake("dash");
    dash.setWordSeparator(org.apache.hop.naming.metadata.NamingWordSeparator.DASH.getCode());
    List<Finding> findings =
        NamingSchemeValidator.validate("order_id", "hop-field", List.of(snake, dash));
    assertEquals(1, findings.size());
    assertEquals("order-id", findings.get(0).getExpected());
  }

  @Test
  void walkerUsesClassLevelKind() {
    NamingScheme scheme = snake("meta");
    scheme.setType(NamingSchemeType.HOP_METADATA.getCode());
    Kinded named = new Kinded();
    named.name = "My Connection";
    List<Finding> findings = NamingSchemeWalker.walk(named, "metadata", List.of(scheme), null);
    assertEquals(1, findings.size());
    assertEquals("my_connection", findings.get(0).getExpected());
    assertEquals("hop-metadata", findings.get(0).getTypeCode());
  }

  @Test
  void walkerFindsTransformInsidePipeline() {
    NamingScheme scheme = snake("fields");
    scheme.setType(NamingSchemeType.HOP_TRANSFORM.getCode());
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("my_pipeline");
    TransformMeta transform = new TransformMeta();
    transform.setName("Read Customers");
    pipeline.addTransform(transform);
    List<Finding> findings =
        NamingSchemeWalker.walk(
            pipeline, "test.hpl", List.of(scheme), java.util.Set.of("hop-transform"));
    assertEquals(1, findings.size());
    assertEquals("Read Customers", findings.get(0).getActual());
    assertEquals("read_customers", findings.get(0).getExpected());
  }

  @Test
  void walkerHonorsTypeFilter() {
    NamingScheme scheme = snake("fields");
    scheme.setType(NamingSchemeType.HOP_TRANSFORM.getCode());
    TransformMeta transform = new TransformMeta();
    transform.setName("Read Customers");
    List<Finding> findings =
        NamingSchemeWalker.walk(
            transform, "test.hpl", List.of(scheme), java.util.Set.of("hop-field"));
    assertTrue(findings.isEmpty());
  }

  @org.apache.hop.core.naming.NamingSchemeKind("hop-metadata")
  private static final class Kinded {
    private String name;

    public String getName() {
      return name;
    }
  }

  private static NamingScheme snake(String name) {
    NamingScheme scheme = new NamingScheme(name);
    scheme.setType(NamingSchemeType.HOP_FIELD.getCode());
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.UNDERSCORE.getCode());
    return scheme;
  }
}
