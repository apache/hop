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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.ImageLoader;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The cluster-mode options of a native Spark run configuration in the run configuration editor. */
@Tag("uitest")
class SparkPipelineRunConfigurationWidgetsTest extends SwtBotTestBase {

  private static final String PARENT = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID;

  @BeforeAll
  static void registerWidgets() {
    for (Field field : SparkPipelineRunConfiguration.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget != null) {
        GuiRegistry.getInstance()
            .addGuiWidgetElement(SparkPipelineRunConfiguration.class.getName(), widget, field);
      }
    }
  }

  @Test
  void clusterModeOptionsRoundTripThroughTheEditorWidgets() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setSparkMaster("spark://spark:7077");
    config.setStagingFolder("staging://hop-staging");
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FormLayout());
          GuiCompositeWidgets composite = new GuiCompositeWidgets(new Variables());
          composite.createCompositeWidgets(config, null, shell, PARENT, null);
          composite.setWidgetsContents(config, shell, PARENT);
          widgets.set(composite);
          shell.setSize(900, 900);
        },
        bot ->
            display.syncExec(
                () -> {
                  Map<String, Control> controls = widgets.get().getWidgetsMap();
                  Combo deployMode = (Combo) controls.get("deployMode");
                  assertNotNull(deployMode, "no deploy mode widget in " + controls.keySet());
                  // A combo of the two descriptions; a new configuration starts in client mode
                  assertArrayEquals(
                      new String[] {
                        SparkDeployMode.CLIENT.getDescription(),
                        SparkDeployMode.CLUSTER.getDescription()
                      },
                      deployMode.getItems());
                  assertEquals(SparkDeployMode.CLIENT.getDescription(), deployMode.getText());
                  TextVar staging = (TextVar) controls.get("stagingFolder");
                  assertNotNull(staging, "no staging folder widget in " + controls.keySet());
                  assertEquals("staging://hop-staging", staging.getText());

                  // Edit as a user would
                  deployMode.select(1);
                  ((TextVar) controls.get("stagingClusterUrl"))
                      .setText("http://seaweedfs:8888/buckets/hop-staging");
                  ((TextVar) controls.get("submitUrl")).setText("http://spark:6066");
                  saveScreenshot(deployMode.getShell());

                  widgets.get().getWidgetsContents(config, PARENT);
                }));

    assertEquals(SparkDeployMode.CLUSTER, config.getDeployMode());
    assertEquals("staging://hop-staging", config.getStagingFolder());
    assertEquals("http://seaweedfs:8888/buckets/hop-staging", config.getStagingClusterUrl());
    assertEquals("http://spark:6066", config.getSubmitUrl());
  }

  /** target/screenshots/SparkPipelineRunConfigurationWidgets.png, to look at the form. */
  private static void saveScreenshot(Shell shell) {
    shell.layout(true, true);
    while (display.readAndDispatch()) {
      // let the form paint
    }
    Rectangle bounds = shell.getBounds();
    Image image = new Image(display, bounds.width, bounds.height);
    GC gc = new GC(display);
    try {
      gc.copyArea(image, bounds.x, bounds.y);
      ImageLoader loader = new ImageLoader();
      loader.data = new ImageData[] {image.getImageData()};
      File folder = new File("target/screenshots");
      folder.mkdirs();
      loader.save(
          new File(folder, "SparkPipelineRunConfigurationWidgets.png").getPath(), SWT.IMAGE_PNG);
    } finally {
      gc.dispose();
      image.dispose();
    }
  }

  /** The options sit on five tabs, in this order, the Spark settings in a table of their own. */
  @Test
  void theOptionsSitOnFiveTabs() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.KUBERNETES);
    config.setSparkConfigs(
        "spark.kubernetes.container.image=myrepo/hop-spark:4.1.3\nspark.sql.shuffle.partitions=200");
    config.setPathSchemeMap("minio=s3a");
    AtomicReference<String[]> tabs = new AtomicReference<>();
    AtomicReference<Shell> scene = new AtomicReference<>();

    withScene(
        shell -> {
          scene.set(shell);
          shell.setLayout(new FormLayout());
          GuiCompositeWidgets composite = new GuiCompositeWidgets(new Variables());
          composite.createCompositeWidgets(config, null, shell, PARENT, null);
          composite.setWidgetsContents(config, shell, PARENT);
          shell.setSize(900, 700);
        },
        bot ->
            display.syncExec(
                () -> {
                  CTabFolder folder = findTabFolder(scene.get());
                  folder.setSelection(3);
                  printToFile(scene.get(), "SparkRunConfigurationSparkSettings.png");
                  folder.setSelection(0);
                  assertNotNull(folder, "no tabs in the run configuration widgets");
                  tabs.set(
                      Arrays.stream(folder.getItems())
                          .map(CTabItem::getText)
                          .toArray(String[]::new));
                  printToFile(scene.get(), "SparkRunConfigurationTabs.png");
                }));

    assertArrayEquals(
        new String[] {"General", "Cluster", "Resources", "Spark settings", "Hop on Spark"},
        tabs.get());
  }

  /**
   * A configuration saved before the cluster type gets the type of its master; choosing another
   * type fills in a master of that type and shows only its options.
   */
  @Test
  void theClusterTypeShowsOnlyItsOptions() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setSparkMaster("yarn");
    AtomicReference<GuiCompositeWidgets> widgets = new AtomicReference<>();
    AtomicReference<Shell> scene = new AtomicReference<>();
    AtomicReference<String> typeShown = new AtomicReference<>();
    AtomicReference<String> masterAfter = new AtomicReference<>();
    Map<String, Boolean> visibleOnYarn = new java.util.HashMap<>();
    Map<String, Boolean> visibleOnKubernetes = new java.util.HashMap<>();

    withScene(
        shell -> {
          scene.set(shell);
          shell.setLayout(new FormLayout());
          GuiCompositeWidgets composite = new GuiCompositeWidgets(new Variables());
          composite.createCompositeWidgets(config, null, shell, PARENT, null);
          composite.setWidgetsContents(config, shell, PARENT);
          SparkRunConfigurationWidgetsListener listener =
              new SparkRunConfigurationWidgetsListener(config);
          composite.setWidgetsListener(listener);
          listener.widgetsPopulated(composite);
          widgets.set(composite);
          shell.setSize(900, 700);
        },
        bot ->
            display.syncExec(
                () -> {
                  Map<String, Control> controls = widgets.get().getWidgetsMap();
                  Combo type =
                      (Combo) controls.get(SparkPipelineRunConfiguration.WIDGET_ID_CLUSTER_TYPE);
                  typeShown.set(type.getText());
                  for (String id : List.of("hadoopConfDir", "submitUrl", "fatJar")) {
                    visibleOnYarn.put(id, controls.get(id).getVisible());
                  }
                  type.setText(SparkClusterType.KUBERNETES.getDescription());
                  type.notifyListeners(SWT.Modify, new org.eclipse.swt.widgets.Event());
                  masterAfter.set(((TextVar) controls.get("sparkMaster")).getText());
                  for (String id : List.of("hadoopConfDir", "submitUrl", "fatJar")) {
                    visibleOnKubernetes.put(id, controls.get(id).getVisible());
                  }
                  CTabFolder folder = findTabFolder(scene.get());
                  folder.setSelection(1);
                  printToFile(scene.get(), "SparkRunConfigurationKubernetesCluster.png");
                  folder.setSelection(0);
                  printToFile(scene.get(), "SparkRunConfigurationKubernetesGeneral.png");
                }));

    assertEquals(SparkClusterType.YARN.getDescription(), typeShown.get());
    assertEquals(Map.of("hadoopConfDir", true, "submitUrl", false, "fatJar", true), visibleOnYarn);
    assertEquals("k8s://https://host:6443", masterAfter.get());
    assertEquals(
        Map.of("hadoopConfDir", false, "submitUrl", false, "fatJar", true), visibleOnKubernetes);
  }

  private static CTabFolder findTabFolder(Composite composite) {
    for (Control child : composite.getChildren()) {
      if (child instanceof CTabFolder folder) {
        return folder;
      }
      if (child instanceof Composite nested) {
        CTabFolder found = findTabFolder(nested);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** target/screenshots/<name>: the shell drawn into an image, which needs no screen capture. */
  private static void printToFile(Shell shell, String name) {
    Rectangle size = shell.getClientArea();
    Image image = new Image(display, size.width, size.height);
    GC gc = new GC(image);
    try {
      shell.print(gc);
      ImageLoader loader = new ImageLoader();
      loader.data = new ImageData[] {image.getImageData()};
      File folder = new File("target/screenshots");
      folder.mkdirs();
      loader.save(new File(folder, name).getPath(), SWT.IMAGE_PNG);
    } finally {
      gc.dispose();
      image.dispose();
    }
  }
}
