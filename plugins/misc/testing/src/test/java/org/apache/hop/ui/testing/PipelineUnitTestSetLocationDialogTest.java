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

package org.apache.hop.ui.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetField;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Guards the design-time cost of the data set location dialog, the dialog opened from the unit test
 * markers on the pipeline canvas.
 *
 * <p>It used to be handed a map holding the output fields of <em>every</em> transform in the
 * pipeline, so opening it had to resolve them all first. A transform that determines its output
 * from a variable - a Table input reading its connection from {@code ${connection}} for example -
 * connects to a database to answer that, on the SWT thread, which froze Hop GUI (issue #8203). The
 * dialog now resolves the fields of the one transform it is mapping, and only when it needs them.
 */
@Tag("uitest")
class PipelineUnitTestSetLocationDialogTest extends SwtBotTestBase {

  private static final String MAPPED_TRANSFORM = "Fonte Sql";
  private static final String DATA_SET_NAME = "raw rows";
  private static final String[] TRANSFORM_NAMES = {
    "Iniciar", "Log Inicial", MAPPED_TRANSFORM, "Salva S3", "Metricas Fluxo", "Log Final"
  };

  /** Counts how often the fields of a transform were resolved, keyed by transform name. */
  private final Map<String, AtomicInteger> resolutions = new ConcurrentHashMap<>();

  @Test
  void openingTheDialogResolvesNoTransformFields() {
    withDialog(
        parent -> newDialog(parent).open(),
        bot -> {
          SWTBot dialogBot = locationShell(bot).bot();
          // Nothing was asked for yet: the dialog is up without a single field lookup.
          assertTrue(
              resolutions.isEmpty(), "Opening the dialog resolved fields: " + resolutions.keySet());
          dialogBot.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertTrue(resolutions.isEmpty(), "Fields resolved: " + resolutions.keySet());
  }

  @Test
  void fieldsOfTheMappedTransformAreResolvedOnceOnDemand() {
    withDialog(
        parent -> newDialog(parent).open(),
        bot -> {
          SWTBotShell locationShell = locationShell(bot);

          // Map fields twice: the second round must be served from the dialog's own cache.
          //
          mapFieldsAndCancel(bot, locationShell);
          mapFieldsAndCancel(bot, locationShell);

          locationShell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(
        List.of(MAPPED_TRANSFORM),
        List.copyOf(resolutions.keySet()),
        "Only the transform being mapped should have its fields resolved");
    assertEquals(
        1, resolutions.get(MAPPED_TRANSFORM).get(), "Resolved fields should be reused, not redone");
  }

  private void mapFieldsAndCancel(SWTBot bot, SWTBotShell locationShell) {
    locationShell
        .bot()
        .button(
            BaseMessages.getString(
                PipelineUnitTestSetLocationDialog.class,
                "PipelineUnitTestSetLocationDialog.MapFields.Button"))
        .click();

    SWTBotShell mappingShell =
        bot.shell(BaseMessages.getString(EnterMappingDialog.class, "EnterMappingDialog.Title"));
    mappingShell.activate();
    mappingShell.bot().button(buttonLabel("System.Button.Cancel")).click();
  }

  private SWTBotShell locationShell(SWTBot bot) {
    SWTBotShell shell =
        bot.shell(
            BaseMessages.getString(
                PipelineUnitTestSetLocationDialog.class,
                "PipelineUnitTestSetLocationDialog.Shell.Title"));
    shell.activate();
    return shell;
  }

  private PipelineUnitTestSetLocationDialog newDialog(org.eclipse.swt.widgets.Shell parent) {
    IVariables variables = new Variables();
    DataSet dataSet = dataSet();
    IHopMetadataProvider metadataProvider = metadataProvider(dataSet);

    PipelineUnitTestSetLocation location = new PipelineUnitTestSetLocation();
    location.setTransformName(MAPPED_TRANSFORM);
    location.setDataSetName(DATA_SET_NAME);

    return new PipelineUnitTestSetLocationDialog(
        parent,
        variables,
        metadataProvider,
        location,
        List.of(dataSet),
        TRANSFORM_NAMES,
        this::resolveFields);
  }

  /** Stands in for {@code PipelineMeta.getTransformFields()}, counting every call. */
  private IRowMeta resolveFields(String transformName) {
    resolutions.computeIfAbsent(transformName, name -> new AtomicInteger()).incrementAndGet();
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static DataSet dataSet() {
    DataSet dataSet = new DataSet();
    dataSet.setName(DATA_SET_NAME);
    dataSet.getFields().add(new DataSetField("id", IValueMeta.TYPE_STRING, 50, -1, null, null));
    dataSet.getFields().add(new DataSetField("name", IValueMeta.TYPE_STRING, 50, -1, null, null));
    return dataSet;
  }

  private static IHopMetadataProvider metadataProvider(DataSet dataSet) {
    try {
      MemoryMetadataProvider provider = new MemoryMetadataProvider();
      provider.getSerializer(DataSet.class).save(dataSet);
      return provider;
    } catch (Exception e) {
      throw new IllegalStateException("Unable to prepare the data set metadata", e);
    }
  }
}
