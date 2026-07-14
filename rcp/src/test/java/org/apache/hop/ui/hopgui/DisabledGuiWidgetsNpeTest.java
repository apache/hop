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

package org.apache.hop.ui.hopgui;

import org.apache.hop.ui.testing.DisabledGuiWidgetsTestBase;
import org.junit.jupiter.api.Tag;

/**
 * Disables every {@code @GuiWidgetElement} that hop-core, hop-engine and hop-ui register - one at a
 * time - and checks that its composite still builds and still survives the listener callbacks.
 *
 * <p>This lives in hop-ui-rcp rather than in hop-ui because the harness needs the standalone
 * look-and-feel implementations (TextSizeUtilFacadeImpl and friends) that this module supplies, and
 * hop-ui cannot depend on them. The widgets that plugins add are covered by the same harness in
 * each plugin's own module: a module only ever sees the widgets on its own classpath.
 *
 * <p>Run: {@code mvn -pl rcp test} (skipped without a display; {@code -XstartOnFirstThread} is
 * added automatically on macOS).
 */
@Tag("uitest")
class DisabledGuiWidgetsNpeTest extends DisabledGuiWidgetsTestBase {
  // No package filter: everything on this module's classpath is hop-core, hop-engine or hop-ui.
}
