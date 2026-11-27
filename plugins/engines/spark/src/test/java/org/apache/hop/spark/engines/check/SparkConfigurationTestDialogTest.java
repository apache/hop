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

package org.apache.hop.spark.engines.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkDeployMode;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.CheckResultDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The results of the configuration test in the check results dialog. */
@Tag("uitest")
class SparkConfigurationTestDialogTest extends SwtBotTestBase {

  @Test
  void theResultsListEveryCheckWithItsFix() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.STANDALONE);
    config.setSparkMaster("spark://spark:7077");
    config.setDeployMode(SparkDeployMode.CLUSTER);
    config.setStagingFolder(System.getProperty("java.io.tmpdir"));
    config.setStagingClusterUrl("minio://hop-staging");
    config.setExecutorMemory("2 gigs");
    List<ICheckResult> results =
        SparkConfigurationChecker.standard().check(config, new Variables(), null);
    String title =
        BaseMessages.getString(SparkConfigurationTestDialog.class, "SparkCheck.Dialog.Title");
    AtomicInteger rows = new AtomicInteger();
    List<String> shown = new java.util.ArrayList<>();

    withDialog(
        parent -> {
          CheckResultDialog dialog = new CheckResultDialog(parent, results);
          dialog.setTitle(title);
          dialog.setSourceColumnLabel(
              BaseMessages.getString(
                  SparkConfigurationTestDialog.class, "SparkCheck.Dialog.Column"));
          dialog.setEditingOrigin(false);
          dialog.open();
        },
        bot -> {
          SWTBotShell dialog = bot.shell(title);
          rows.set(dialog.bot().table().rowCount());
          SWTBotTable table = dialog.bot().table();
          for (int row = 0; row < table.rowCount(); row++) {
            shown.add(table.cell(row, 1) + " | " + table.cell(row, 2) + " | " + table.cell(row, 3));
          }
          dialog.close();
        });

    // With errors the dialog lists the checks that did not pass; the passed ones are one click away
    shown.forEach(System.out::println);
    assertTrue(shown.stream().anyMatch(line -> line.contains("Fix:")), String.valueOf(shown));
    assertEquals(
        results.stream().filter(r -> r.getType() != ICheckResult.TYPE_RESULT_OK).count(),
        rows.get());
  }
}
