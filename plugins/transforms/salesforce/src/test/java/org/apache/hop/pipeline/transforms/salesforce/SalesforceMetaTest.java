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

package org.apache.hop.pipeline.transforms.salesforce;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

public class SalesforceMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  public static List<String> getDefaultAttributes() {
    return Arrays.asList("targetUrl", "username", "password", "timeout", "compression", "module");
  }

  @Test
  void testBaseCheck() {
    SalesforceTransformMeta meta = mock(SalesforceTransformMeta.class, Mockito.CALLS_REAL_METHODS);
    meta.setDefault();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, null, null);
    boolean hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertTrue(hasError);
    remarks.clear();

    meta.setDefault();
    meta.setUsername("anonymous");
    meta.check(remarks, null, null, null, null, null, null, null, null);
    hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertFalse(hasError);
    remarks.clear();

    meta.setDefault();
    meta.setTargetUrl(null);
    meta.setUsername("anonymous");
    meta.setPassword("password");
    meta.check(remarks, null, null, null, null, null, null, null, null);
    hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertTrue(hasError);
    remarks.clear();

    meta.setDefault();
    meta.setUsername("anonymous");
    meta.setModule(null);
    meta.check(remarks, null, null, null, null, null, null, null, null);
    hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertTrue(hasError);
  }
}
