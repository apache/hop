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

package org.apache.hop.pipeline.transforms.sql;

import java.util.List;
import org.apache.hop.ui.testing.DisabledGuiWidgetsTestBase;
import org.junit.jupiter.api.Tag;

/**
 * Disables every {@code @GuiWidgetElement} this plugin registers, one at a time, and checks that
 * its composite still builds and still survives the listener callbacks. See {@link
 * DisabledGuiWidgetsTestBase}.
 */
@Tag("uitest")
class ExecSqlDisabledWidgetsTest extends DisabledGuiWidgetsTestBase {

  @Override
  protected List<String> packagesUnderTest() {
    return List.of("org.apache.hop.pipeline.transforms.sql");
  }
}
