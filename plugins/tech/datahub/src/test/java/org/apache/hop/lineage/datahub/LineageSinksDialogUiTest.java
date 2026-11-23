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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.hub.LineageConfiguration;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.spi.ILineageSink;
import org.apache.hop.lineage.spi.LineageSinkHealth;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.lineage.LineageSinksDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tools > Lineage sinks shows every sink with its counts, and why one could not start. */
@Tag("uitest")
class LineageSinksDialogUiTest extends SwtBotTestBase {

  private static final Class<?> PKG = LineageSinksDialog.class;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void everySinkIsShownWithItsCountsAndItsLastError() {
    ILineageSink variablesSink =
        new ILineageSink() {
          @Override
          public void accept(List<LineageEvent> events) {
            // delivered
          }

          @Override
          public LineageSinkHealth health() {
            return new LineageSinkHealth(42, 3, 1, 0, 5, 7, 1L, 2L, "HTTP 401 from http://gms");
          }
        };
    LineageHub hub =
        LineageHub.newIsolatedForTesting(
            LineageConfiguration.forTesting(true, 100, 10, 50L, Set.of()),
            List.of(variablesSink),
            target -> {
              throw new HopException("the trust store is missing");
            });
    String key =
        hub.useSink(new LineageSinkTarget("prod-datahub", "datahub", Map.of("URL", "http://gms")));
    hub.emit(
        LineageEvent.of(
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder().sinks(Set.of(key)).build(),
            null));
    hub.flush();

    try {
      withDialog(
          parent -> new LineageSinksDialog(parent, new Variables(), hub).open(),
          bot -> {
            bot.shell(BaseMessages.getString(PKG, "LineageSinksDialog.Title")).activate();
            SWTBotTable table = bot.table();
            assertEquals(2, table.rowCount());
            // Column 0 is the row number.
            assertEquals("42", table.cell(0, 5));
            assertEquals("7", table.cell(0, 10));
            assertEquals("HTTP 401 from http://gms", table.cell(0, 13));
            assertEquals("prod-datahub", table.cell(1, 1));
            assertEquals(
                BaseMessages.getString(PKG, "LineageSinksDialog.State.Failed"), table.cell(1, 4));
            assertTrue(table.cell(1, 13).contains("the trust store is missing"));
            bot.button(buttonLabel("System.Button.Close")).click();
          });
    } finally {
      hub.shutdown();
    }
  }
}
