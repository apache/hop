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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.junit.jupiter.api.Test;

class VfsBrowseRootsTest {

  @Test
  void namedRootUsesThreeSlashes() {
    assertEquals("prod:///", VfsBrowseRoots.namedConnectionRoot(null, "prod"));
  }

  @Test
  void namedRootResolvesVariables() {
    IVariables variables = new Variables();
    variables.setVariable("CONN", "warehouse");
    assertEquals("warehouse:///", VfsBrowseRoots.namedConnectionRoot(variables, "${CONN}"));
  }

  @Test
  void namedRootIsNullWithoutAName() {
    assertNull(VfsBrowseRoots.namedConnectionRoot(new Variables(), null));
    assertNull(VfsBrowseRoots.namedConnectionRoot(new Variables(), "  "));
    IVariables variables = new Variables();
    variables.setVariable("CONN", "");
    assertNull(VfsBrowseRoots.namedConnectionRoot(variables, "${CONN}"));
  }

  @Test
  void browseLocationUsesTheMetadataName() {
    NamedLocation location = new NamedLocation();
    location.setName("my-s3");
    assertEquals("my-s3:///", location.getBrowseRoot(new Variables()));
    location.setName(null);
    assertNull(location.getBrowseRoot(new Variables()));
  }

  private static final class NamedLocation extends HopMetadataBase implements IVfsBrowseLocation {}
}
