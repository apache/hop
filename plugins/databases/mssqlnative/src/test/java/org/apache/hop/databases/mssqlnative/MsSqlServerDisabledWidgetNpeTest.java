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

package org.apache.hop.databases.mssqlnative;

import java.util.List;
import org.apache.hop.ui.testing.DisabledGuiWidgetsTestBase;
import org.junit.jupiter.api.Tag;

/**
 * Disables the widgets of the MS SQL Server database metas, one at a time.
 *
 * <p>This is the case that put the harness here: {@code
 * MsSqlServerNativeDatabaseMeta.enableField()} used to look the {@code usingIntegratedSecurity}
 * checkbox up by id and call {@code getSelection()} on it without a null check - the username and
 * password lookups right above it <em>were</em> guarded - so excluding that one checkbox in
 * disabledGuiElements.xml turned the database dialog into a NullPointerException.
 */
@Tag("uitest")
class MsSqlServerDisabledWidgetNpeTest extends DisabledGuiWidgetsTestBase {

  @Override
  protected List<String> packagesUnderTest() {
    // The hop-core/hop-engine/hop-ui widgets that also sit on this module's classpath are covered
    // by the same harness in hop-ui-rcp.
    //
    return List.of("org.apache.hop.databases");
  }
}
