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

package org.apache.hop.pipeline.transforms.csvinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.ISchemaCapability;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputDialog;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputMeta;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.datacatalog.CatalogFieldsDialog;
import org.apache.hop.ui.datacatalog.CatalogFieldsSource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiImpl;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The CSV file input and Text file input dialogs fill their field table from the schema a data
 * catalog has for their file.
 */
@Tag("uitest")
class GetFieldsFromCatalogDialogTest extends SwtBotTestBase {

  private static final String FILE = "/data/in/customers.csv";

  private static final String CATALOG_TITLE =
      BaseMessages.getString(CatalogFieldsDialog.class, "CatalogFieldsDialog.Title");

  /** Name, type, format, length and precision of the three fields of the dataset. */
  private static final List<List<String>> EXPECTED =
      List.of(
          List.of("id", "Integer", "", "", "0"),
          List.of("email", "String", "", "255", ""),
          List.of("signed_up", "Date", "", "", ""));

  private static MemoryMetadataProvider metadataProvider;

  /** The annotated widgets are looked up by class name; register them as the plugin scan does. */
  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(CatalogFieldsSource.class.getName(), CatalogFieldsSource.PARENT_ID)
        != null) {
      return;
    }
    for (Field field : CatalogFieldsSource.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(CatalogFieldsSource.class.getName(), element, field);
      }
    }
  }

  /** The metadata selection lines read their provider from the running Hop GUI. */
  @BeforeAll
  static void installHopGui() throws Exception {
    metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(DataCatalog.class)
        .save(new DataCatalog("catalog", null, new SchemaCatalog()));
    Variables variables = new Variables();
    HopGui hopGui = mock(HopGui.class);
    when(hopGui.getMetadataProvider())
        .thenReturn(new MultiMetadataProvider(variables, metadataProvider));
    when(hopGui.getVariables()).thenReturn(variables);
    setHopGui(hopGui);
  }

  @AfterAll
  static void removeHopGui() throws Exception {
    setHopGui(null);
  }

  private static void setHopGui(HopGui hopGui) throws Exception {
    Field instance = HopGuiImpl.class.getDeclaredField("instance");
    instance.setAccessible(true);
    instance.set(null, hopGui);
  }

  @BeforeEach
  void forget() {
    SchemaCatalog.asked = null;
  }

  @Test
  void csvFileInputFillsItsFieldsFromTheCatalog() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setDefault();
    meta.setFilename(FILE);
    PipelineMeta pipelineMeta = pipelineWith(meta, "CSV file input");
    List<List<String>> rows = new ArrayList<>();

    withDialog(
        parent -> new CsvInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBotShell dialog =
              bot.shell(BaseMessages.getString(CsvInput.class, "CsvInputDialog.Shell.Title"));
          dialog.activate();
          dialog
              .bot()
              .button(
                  BaseMessages.getString(
                      CsvInput.class, "CsvInputDialog.GetFieldsFromCatalog.Button"))
              .click();
          getFieldsFromCatalog(bot, "csv");
          rows.addAll(readFields(bot, dialog, shell -> onlyTable(shell)));
          dialog.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(EXPECTED, rows);
    assertNotNull(SchemaCatalog.asked, "the catalog was not asked");
    assertTrue(
        SchemaCatalog.asked.name().endsWith(FILE),
        "the dataset starts as the file: " + SchemaCatalog.asked);
  }

  @Test
  void textFileInputFillsItsFieldsFromTheCatalog() {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.setDefault();
    InputFile file = new InputFile();
    file.setFileName(FILE);
    meta.getFileInput().getInputFiles().add(file);
    PipelineMeta pipelineMeta = pipelineWith(meta, "Text file input");
    String fieldsTab =
        BaseMessages.getString(TextFileInputMeta.class, "TextFileInputDialog.FieldsTab.TabTitle");
    List<List<String>> rows = new ArrayList<>();

    withDialog(
        parent -> new TextFileInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBotShell dialog =
              bot.shell(
                  BaseMessages.getString(
                      TextFileInputMeta.class, "TextFileInputDialog.DialogTitle"));
          dialog.activate();
          display.syncExec(() -> selectTab(dialog.widget, fieldsTab));
          dialog
              .bot()
              .button(
                  BaseMessages.getString(
                      TextFileInputMeta.class, "TextFileInputDialog.GetFieldsFromCatalog.Button"))
              .click();
          getFieldsFromCatalog(bot, "text");
          rows.addAll(readFields(bot, dialog, shell -> tableOnTab(shell, fieldsTab)));
          dialog.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    // Text file input has a position column between the format and the length
    assertEquals(EXPECTED, rows);
    assertTrue(SchemaCatalog.asked.name().endsWith(FILE), String.valueOf(SchemaCatalog.asked));
  }

  @Test
  void csvFileInputDisablesTheCatalogButtonWhenItIgnoresTheFields() {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setDefault();
    meta.setFilename(FILE);
    meta.setIgnoreFields(true);
    PipelineMeta pipelineMeta = pipelineWith(meta, "CSV file input");
    boolean[] enabled = new boolean[1];

    withDialog(
        parent -> new CsvInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBotShell dialog =
              bot.shell(BaseMessages.getString(CsvInput.class, "CsvInputDialog.Shell.Title"));
          dialog.activate();
          enabled[0] =
              dialog
                  .bot()
                  .button(
                      BaseMessages.getString(
                          CsvInput.class, "CsvInputDialog.GetFieldsFromCatalog.Button"))
                  .isEnabled();
          dialog.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertFalse(enabled[0], "the button does nothing while the fields are ignored");
  }

  @Test
  void textFileInputLooksUpTheFirstFileAFolderWildcardMatches(@TempDir Path folder)
      throws Exception {
    Files.writeString(folder.resolve("customers.csv"), "id;email;signed_up\n");
    Files.writeString(folder.resolve("notes.txt"), "not read\n");
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.setDefault();
    InputFile file = new InputFile();
    file.setFileName(folder.toString());
    file.setFileMask(".*\\.csv");
    meta.getFileInput().getInputFiles().add(file);
    PipelineMeta pipelineMeta = pipelineWith(meta, "Text file input");
    String fieldsTab =
        BaseMessages.getString(TextFileInputMeta.class, "TextFileInputDialog.FieldsTab.TabTitle");

    withDialog(
        parent -> new TextFileInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBotShell dialog =
              bot.shell(
                  BaseMessages.getString(
                      TextFileInputMeta.class, "TextFileInputDialog.DialogTitle"));
          dialog.activate();
          display.syncExec(() -> selectTab(dialog.widget, fieldsTab));
          dialog
              .bot()
              .button(
                  BaseMessages.getString(
                      TextFileInputMeta.class, "TextFileInputDialog.GetFieldsFromCatalog.Button"))
              .click();
          getFieldsFromCatalog(bot, "text-wildcard");
          readFields(bot, dialog, shell -> tableOnTab(shell, fieldsTab));
          dialog.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertNotNull(SchemaCatalog.asked, "the catalog was not asked");
    assertTrue(
        SchemaCatalog.asked.name().endsWith("/customers.csv"),
        "the dataset is the file the wildcard matches, not the folder: " + SchemaCatalog.asked);
  }

  /** Accepts the catalog and dataset the dialog starts with. */
  private static void getFieldsFromCatalog(SWTBot bot, String name) {
    SWTBotShell catalogDialog = bot.shell(CATALOG_TITLE);
    catalogDialog.activate();
    String folder = System.getProperty("catalogFields.screenshots");
    if (folder != null) {
      SWTUtils.captureScreenshot(folder + "/catalog-fields-" + name + ".png");
    }
    catalogDialog.bot().button(buttonLabel("System.Button.OK")).click();
  }

  /** The name, type, format, length and precision of each field once the table is filled. */
  private static List<List<String>> readFields(
      SWTBot bot, SWTBotShell dialog, Function<Shell, TableView> tableOf) {
    List<List<String>> rows = new ArrayList<>();
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            int[] count = new int[1];
            display.syncExec(() -> count[0] = tableOf.apply(dialog.widget).nrNonEmpty());
            return count[0] == EXPECTED.size();
          }

          @Override
          public String getFailureMessage() {
            return "The field table was not filled from the catalog";
          }
        });
    display.syncExec(
        () -> {
          TableView table = tableOf.apply(dialog.widget);
          boolean position = table.getColumns().length > 9;
          for (int i = 0; i < table.nrNonEmpty(); i++) {
            TableItem item = table.getNonEmpty(i);
            int length = position ? 5 : 4;
            rows.add(
                List.of(
                    item.getText(1),
                    item.getText(2),
                    item.getText(3),
                    item.getText(length),
                    item.getText(length + 1)));
          }
        });
    return rows;
  }

  private static TableView onlyTable(Shell shell) {
    List<TableView> tables = new ArrayList<>();
    collect(shell, tables);
    assertEquals(1, tables.size(), "CSV file input has one table");
    return tables.get(0);
  }

  private static void selectTab(Shell shell, String tab) {
    List<CTabFolder> folders = new ArrayList<>();
    collectFolders(shell, folders);
    for (CTabFolder folder : folders) {
      for (CTabItem item : folder.getItems()) {
        if (tab.equals(item.getText().trim())) {
          folder.setSelection(item);
          shell.layout(true, true);
          return;
        }
      }
    }
    throw new AssertionError("No tab " + tab);
  }

  private static TableView tableOnTab(Shell shell, String tab) {
    List<CTabFolder> folders = new ArrayList<>();
    collectFolders(shell, folders);
    for (CTabFolder folder : folders) {
      for (CTabItem item : folder.getItems()) {
        if (tab.equals(item.getText().trim())) {
          List<TableView> tables = new ArrayList<>();
          collect((Composite) item.getControl(), tables);
          return tables.get(0);
        }
      }
    }
    throw new AssertionError("No tab " + tab);
  }

  private static void collect(Composite parent, List<TableView> tables) {
    for (Control child : parent.getChildren()) {
      if (child instanceof TableView table) {
        tables.add(table);
      } else if (child instanceof Composite composite) {
        collect(composite, tables);
      }
    }
  }

  private static void collectFolders(Composite parent, List<CTabFolder> folders) {
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder folder) {
        folders.add(folder);
      }
      if (child instanceof Composite composite) {
        collectFolders(composite, folders);
      }
    }
  }

  private static PipelineMeta pipelineWith(ITransformMeta meta, String name) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, name + " must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, name, meta));
    pipelineMeta.setMetadataProvider(metadataProvider);
    return pipelineMeta;
  }

  /** A data catalog with one dataset of three fields, which remembers what it was asked. */
  public static class SchemaCatalog implements IDataCatalog, ISchemaCapability {
    static DatasetRef asked;

    private String pluginId = "schema";
    private String pluginName = "Schema catalog";

    @Override
    public String getPluginId() {
      return pluginId;
    }

    @Override
    public void setPluginId(String pluginId) {
      this.pluginId = pluginId;
    }

    @Override
    public String getPluginName() {
      return pluginName;
    }

    @Override
    public void setPluginName(String pluginName) {
      this.pluginName = pluginName;
    }

    @Override
    public SchemaCatalog clone() {
      return new SchemaCatalog();
    }

    @Override
    public void initialize(IVariables variables, IHopMetadataProvider metadataProvider) {
      // Nothing to prepare
    }

    @Override
    public CatalogAnswer<String> ping() {
      return CatalogAnswer.known("schema");
    }

    @Override
    public CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset) {
      asked = dataset;
      return CatalogAnswer.known(new CatalogEntityId("urn:customers"));
    }

    @Override
    public CatalogAnswer<List<CatalogField>> schema(CatalogEntityId id) {
      return CatalogAnswer.known(
          List.of(
              new CatalogField("id", "int", "NUMBER", false, Set.of(), Set.of()),
              new CatalogField("email", "VARCHAR(255)", "STRING", true, Set.of(), Set.of()),
              new CatalogField("signed_up", "date", "DATE", true, Set.of(), Set.of())));
    }

    @Override
    public void close() {
      // Nothing to release
    }
  }
}
