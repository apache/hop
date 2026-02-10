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

package org.apache.hop.ui.core.gui;

import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.ISingletonProvider;
import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;

/*
 * colors etc. are allocated once and released once at the end of the program.
 *
 */
public class GuiResource {

  private static final String CONST_FOR_PLUGIN = "] for plugin ";
  private static final String CONST_ERROR_OCCURRED_LOADING_IMAGE = "Error occurred loading image [";
  private static final ILogChannel log = LogChannel.UI;

  private final Display display;

  private double zoomFactor;

  // Colors
  //
  @Getter private Color colorBackground;
  @Getter private Color colorGraph;
  @Getter private Color colorTab;
  @Getter private Color colorRed;
  @Getter private Color colorDarkRed;
  @Getter private Color colorSuccessGreen;
  @Getter private Color colorBlueCustomGrid;
  @Getter private Color colorGreen;
  @Getter private Color colorDarkGreen;
  @Getter private Color colorBlue;
  @Getter private Color colorOrange;
  @Getter private Color colorYellow;
  @Getter private Color colorMagenta;
  @Getter private Color colorPurple;
  @Getter private Color colorIndigo;
  @Getter private Color colorBlack;
  @Getter private Color colorGray;
  @Getter private Color colorDarkGray;
  @Getter private Color colorVeryDarkGray;
  @Getter private Color colorLightGray;
  @Getter private Color colorDemoGray;
  @Getter private Color colorWhite;
  @Getter private Color colorDirectory;
  @Getter private Color colorHop;
  @Getter private Color colorLight;
  @Getter private Color colorCream;
  @Getter private Color colorLightBlue;
  @Getter private Color colorCrystalText;
  @Getter private Color colorHopDefault;
  @Getter private Color colorHopTrue;
  @Getter private Color colorDeprecated;

  /** Muted variants for use on grayed (non-openable) explorer items; light and dark mode aware. */
  @Getter private Color colorDarkGreenMuted;

  @Getter private Color colorLightBlueMuted;
  @Getter private Color colorRedMuted;
  @Getter private Color colorDarkGrayMuted;
  @Getter private Color colorBlackMuted;

  // Fonts
  //
  private ManagedFont fontDefault;
  private ManagedFont fontGraph;
  private ManagedFont fontNote;
  private ManagedFont fontFixed;
  private ManagedFont fontMedium;
  private ManagedFont fontMediumBold;
  private ManagedFont fontLarge;
  private ManagedFont fontTiny;
  private ManagedFont fontSmall;
  private ManagedFont fontBold;

  // Images
  //
  private Map<String, SwtUniversalImage> imagesTransforms;
  private Map<String, SwtUniversalImage> imagesActions;
  private Map<String, Image> imagesValueMeta;
  private Map<String, Image> imagesDatabase;

  private SwtUniversalImage imageLogo;
  private SwtUniversalImage imageDatabase;
  private SwtUniversalImage imageData;
  private SwtUniversalImage imagePreview;
  private SwtUniversalImage imageMissing;
  private SwtUniversalImage imageDeprecated;
  private SwtUniversalImage imageVariable;
  private SwtUniversalImage imagePipeline;
  private SwtUniversalImage imagePartitionSchema;
  private SwtUniversalImage imageWorkflow;
  private SwtUniversalImage imageArrowDefault;
  private SwtUniversalImage imageArrowTrue;
  private SwtUniversalImage imageArrowFalse;
  private SwtUniversalImage imageArrowError;
  private SwtUniversalImage imageArrowDisabled;
  private SwtUniversalImage imageArrowCandidate;
  private SwtUniversalImage imageServer;
  private SwtUniversalImage imageFolder;
  private SwtUniversalImage imageFile;
  private SwtUniversalImage imageEdit;
  private SwtUniversalImage imageCopyRows;
  private SwtUniversalImage imageCopyRowsDisabled;
  private SwtUniversalImage imageFailure;
  private SwtUniversalImage imageSuccess;
  private SwtUniversalImage imageError;
  private SwtUniversalImage imageErrorDisabled;
  private SwtUniversalImage imageInfo;
  private SwtUniversalImage imageInfoDisabled;
  private SwtUniversalImage imageWarning;
  private SwtUniversalImage imageInput;
  private SwtUniversalImage imageOutput;
  private SwtUniversalImage imageTarget;
  private SwtUniversalImage imageTargetDisabled;
  private SwtUniversalImage imageLocked;
  private SwtUniversalImage imageTrue;
  private SwtUniversalImage imageTrueDisabled;
  private SwtUniversalImage imageFalse;
  private SwtUniversalImage imageFalseDisabled;
  private SwtUniversalImage imageUnconditional;
  private SwtUniversalImage imageUnconditionalDisabled;
  private SwtUniversalImage imageParallel;
  private SwtUniversalImage imageParallelDisabled;
  private SwtUniversalImage imageBusy;
  private SwtUniversalImage imageWaiting;
  private SwtUniversalImage imageInject;
  private SwtUniversalImage imageBalance;
  private SwtUniversalImage imageCheckpoint;

  @Getter private Image imageAdd;
  @Getter private Image imageAddAll;
  @Getter private Image imageAddAbove;
  @Getter private Image imageAddBelow;
  @Getter private Image imageAddSingle;
  @Getter private Image imageCalendar;
  @Getter private Image imageCancel;
  @Getter private Image imageCatalog;
  @Getter private Image imageCheck;
  @Getter private Image imageClear;
  @Getter private Image imageClose;
  @Getter private Image imageCollapseAll;
  @Getter private Image imageColor;
  @Getter private Image imageCopy;
  @Getter private Image imageCut;
  @Getter private Image imageDelete;
  @Getter private Image imageDown;
  @Getter private Image imageDuplicate;
  @Getter private Image imageEmpty;
  @Getter private Image imageExpandAll;
  @Getter private Image imageFunction;
  @Getter private Image imageKeyboard;
  @Getter private Image imageHelp;
  @Getter private Image imageHide;
  @Getter private Image imageHideResults;
  @Getter private Image imageHome;
  @Getter private Image imageLabel;
  @Getter private Image imageLocation;
  @Getter private Image imageMaximizePanel;
  @Getter private Image imageMinimizePanel;
  @Getter private Image imageNavigateBack;
  @Getter private Image imageNavigateForward;
  @Getter private Image imageNavigateUp;
  @Getter private Image imageNew;
  @Getter private Image imageNote;
  @Getter private Image imageOptions;
  @Getter private Image imagePaste;
  @Getter private Image imagePause;
  @Getter private Image imagePlugin;
  @Getter private Image imagePrint;
  @Getter private Image imageRedo;
  @Getter private Image imageRefresh;
  @Getter private Image imageRegex;
  @Getter private Image imageRemoveAll;
  @Getter private Image imageRemoveSingle;
  @Getter private Image imageRename;
  @Getter private Image imageResetOption;
  @Getter private Image imageRotateLeft;
  @Getter private Image imageRotateRight;
  @Getter private Image imageRun;
  @Getter private Image imageSchema;
  @Getter private Image imageSearch;
  @Getter private Image imageSelectAll;
  @Getter private Image imageShow;
  @Getter private Image imageShowAll;
  @Getter private Image imageShowErrorLines;
  @Getter private Image imageShowGrid;
  @Getter private Image imageShowHistory;
  @Getter private Image imageShowLog;
  @Getter private Image imageShowPerf;
  @Getter private Image imageShowResults;
  @Getter private Image imageShowSelected;
  @Getter private Image imageStop;
  @Getter private Image imageSynonym;
  @Getter private Image imageTable;
  @Getter private Image imageUndo;
  @Getter private Image imageUnselectAll;
  @Getter private Image imageUp;
  @Getter private Image imageUser;
  @Getter private Image imageView;

  private Map<String, Image> imageMap;

  private Map<RGB, Color> colorMap;

  /**
   * GuiResource also contains the clipboard as it has to be allocated only once! I don't want to
   * put it in a separate singleton just for this one member.
   */
  private Clipboard clipboard;

  protected GuiResource() {
    this(Display.getCurrent());
  }

  private GuiResource(Display display) {
    this.display = display;

    getResources();

    display.addListener(SWT.Dispose, event -> dispose());

    clipboard = null;

    // Reload images as required by changes in the plugins
    PluginRegistry.getInstance()
        .addPluginListener(
            TransformPluginType.class,
            new IPluginTypeListener() {
              @Override
              public void pluginAdded(Object serviceObject) {
                loadTransformImages();
              }

              @Override
              public void pluginRemoved(Object serviceObject) {
                loadTransformImages();
              }

              @Override
              public void pluginChanged(Object serviceObject) {
                // Do Nothing
              }
            });

    PluginRegistry.getInstance()
        .addPluginListener(
            ActionPluginType.class,
            new IPluginTypeListener() {
              @Override
              public void pluginAdded(Object serviceObject) {
                // make sure we load up the images for any new actions that have been registered
                loadActionImages();
              }

              @Override
              public void pluginRemoved(Object serviceObject) {
                // rebuild the image map, in effect removing the image(s) for actions that have gone
                // away
                loadActionImages();
              }

              @Override
              public void pluginChanged(Object serviceObject) {
                // nothing needed here
              }
            });
  }

  private static final ISingletonProvider PROVIDER;

  static {
    PROVIDER = (ISingletonProvider) ImplementationLoader.newInstance(GuiResource.class);
  }

  public static GuiResource getInstance() {
    return (GuiResource) PROVIDER.getInstanceInternal();
  }

  /** reloads all colors, fonts and images. */
  public void reload() {
    // Let's not dispose of all colors etc. since they'll still be in use by the GUI.
    // It's better to leak those few things than to crash the GUI since this is exceptional anyway.
    //

    // Clear the image map. This forces toolbar icons and so on to be re-created.
    // This again leaks images. This is not meant to be repeated a lot.
    //
    imageMap.clear();

    // Re-calculate the native zoom
    //
    PropsUi.getInstance().reCalculateNativeZoomFactor();

    // Re-load colors, fonts and images.
    //
    getResources();
  }

  private void getResources() {
    PropsUi props = PropsUi.getInstance();
    zoomFactor = props.getZoomFactor();
    imageMap = new HashMap<>();
    colorMap = new HashMap<>();

    // It is recommended not to use the Device to create the color, but the RAP needs a screen (to
    // be removed if the RAP evolves)!
    colorBackground = new Color(display, props.contrastColor(new RGB(240, 240, 240)));
    colorGraph = new Color(display, props.contrastColor(new RGB(235, 235, 235)));
    colorTab = new Color(display, props.contrastColor(new RGB(128, 128, 128)));
    colorSuccessGreen = new Color(display, props.contrastColor(0, 139, 0));
    colorRed = new Color(display, props.contrastColor(255, 0, 0));
    colorDarkRed = new Color(display, props.contrastColor(192, 57, 43));
    colorGreen = new Color(display, props.contrastColor(0, 255, 0));
    colorDarkGreen = new Color(display, props.contrastColor(16, 172, 132));
    colorBlue = new Color(display, props.contrastColor(0, 0, 255));
    colorYellow = new Color(display, props.contrastColor(255, 255, 0));
    colorMagenta = new Color(display, props.contrastColor(255, 0, 255));
    colorPurple = new Color(display, props.contrastColor(128, 0, 128));
    colorIndigo = new Color(display, props.contrastColor(75, 0, 130));
    colorOrange = new Color(display, props.contrastColor(255, 165, 0));
    colorBlueCustomGrid = new Color(display, props.contrastColor(240, 248, 255));
    colorWhite = new Color(display, props.contrastColor(254, 254, 254));
    colorDemoGray = new Color(display, props.contrastColor(240, 240, 240));
    colorLightGray = new Color(display, props.contrastColor(225, 225, 225));
    colorGray = new Color(display, props.contrastColor(215, 215, 215));
    colorDarkGray = new Color(display, props.contrastColor(100, 100, 100));
    colorVeryDarkGray = new Color(display, props.contrastColor(50, 50, 50));
    colorBlack = new Color(display, props.contrastColor(0, 0, 0));
    colorLightBlue = new Color(display, props.contrastColor(135, 206, 250)); // light sky blue
    colorDirectory = new Color(display, props.contrastColor(0, 0, 255));
    colorHop = new Color(display, props.contrastColor(188, 198, 82));
    colorLight = new Color(display, props.contrastColor(238, 248, 152));
    colorCream = new Color(display, props.contrastColor(248, 246, 231));
    colorCrystalText = new Color(display, props.contrastColor(61, 99, 128));
    colorHopDefault = new Color(display, props.contrastColor(61, 99, 128));
    colorHopTrue = new Color(display, props.contrastColor(12, 178, 15));
    colorDeprecated = new Color(display, props.contrastColor(246, 196, 56));

    // Muted variants for grayed (non-openable) explorer items; dark mode variants in PropsUi
    colorDarkGreenMuted = new Color(display, props.contrastColor(new RGB(85, 115, 85)));
    colorLightBlueMuted = new Color(display, props.contrastColor(new RGB(75, 95, 165)));
    colorRedMuted = new Color(display, props.contrastColor(new RGB(130, 85, 85)));
    colorDarkGrayMuted = new Color(display, props.contrastColor(new RGB(105, 105, 105)));
    colorBlackMuted = new Color(display, props.contrastColor(new RGB(90, 90, 90)));

    // Load all images from files...
    loadFonts();
    loadCommonImages();
    loadTransformImages();
    loadActionImages();
    loadValueMetaImages();
    loadDatabaseImages();
  }

  private void dispose() {
    // display shutdown, clean up the fonts, images, colors, and so on.
    //
    // Fonts
    //
    fontDefault.dispose();
    fontGraph.dispose();
    fontNote.dispose();
    fontFixed.dispose();
    fontMedium.dispose();
    fontMediumBold.dispose();
    fontLarge.dispose();
    fontTiny.dispose();
    fontSmall.dispose();
    fontBold.dispose();

    // Common images
    imageLogo.dispose();
    imageDatabase.dispose();
    imageData.dispose();
    imagePreview.dispose();
    imageServer.dispose();
    imageFile.dispose();
    imageFolder.dispose();
    imageMissing.dispose();
    imageVariable.dispose();
    imagePipeline.dispose();
    imagePartitionSchema.dispose();
    imageWorkflow.dispose();
    imageCopyRows.dispose();
    imageCopyRowsDisabled.dispose();
    imageError.dispose();
    imageErrorDisabled.dispose();
    imageInfo.dispose();
    imageInfoDisabled.dispose();
    imageWarning.dispose();
    imageDeprecated.dispose();
    imageExpandAll.dispose();
    imageSearch.dispose();
    imageRegex.dispose();
    imageNew.dispose();
    imageEdit.dispose();
    imageLocked.dispose();
    imageInput.dispose();
    imageOutput.dispose();
    imageTarget.dispose();
    imageTargetDisabled.dispose();
    imageTrue.dispose();
    imageTrueDisabled.dispose();
    imageFalse.dispose();
    imageFalseDisabled.dispose();
    imageFailure.dispose();
    imageSuccess.dispose();
    imageParallel.dispose();
    imageParallelDisabled.dispose();
    imageUnconditional.dispose();
    imageUnconditionalDisabled.dispose();
    imageBusy.dispose();
    imageInject.dispose();
    imageBalance.dispose();
    imageCheckpoint.dispose();
    imageArrowDefault.dispose();
    imageArrowTrue.dispose();
    imageArrowFalse.dispose();
    imageArrowError.dispose();
    imageArrowDisabled.dispose();
    imageArrowCandidate.dispose();

    // Small images 16x16
    disposeImage(imageAdd);
    disposeImage(imageAddAll);
    disposeImage(imageAddAbove);
    disposeImage(imageAddBelow);
    disposeImage(imageAddSingle);
    disposeImage(imageCalendar);
    disposeImage(imageCancel);
    disposeImage(imageCatalog);
    disposeImage(imageCheck);
    disposeImage(imageClear);
    disposeImage(imageClose);
    disposeImage(imageCollapseAll);
    disposeImage(imageColor);
    disposeImage(imageCopy);
    disposeImage(imageCut);
    disposeImage(imageDelete);
    disposeImage(imageDown);
    disposeImage(imageDuplicate);
    disposeImage(imageFunction);
    disposeImage(imageKeyboard);
    disposeImage(imageHelp);
    disposeImage(imageHide);
    disposeImage(imageHideResults);
    disposeImage(imageHome);
    disposeImage(imageLabel);
    disposeImage(imageLocation);
    disposeImage(imageMaximizePanel);
    disposeImage(imageMinimizePanel);
    disposeImage(imageNavigateBack);
    disposeImage(imageNavigateForward);
    disposeImage(imageNavigateUp);
    disposeImage(imageNote);
    disposeImage(imagePaste);
    disposeImage(imagePause);
    disposeImage(imagePlugin);
    disposeImage(imagePrint);
    disposeImage(imageRedo);
    disposeImage(imageRefresh);
    disposeImage(imageRemoveAll);
    disposeImage(imageRemoveSingle);
    disposeImage(imageRename);
    disposeImage(imageResetOption);
    disposeImage(imageRotateLeft);
    disposeImage(imageRotateRight);
    disposeImage(imageRun);
    disposeImage(imageSchema);
    disposeImage(imageSearch);
    disposeImage(imageSelectAll);
    disposeImage(imageShow);
    disposeImage(imageShowAll);
    disposeImage(imageShowErrorLines);
    disposeImage(imageShowGrid);
    disposeImage(imageShowHistory);
    disposeImage(imageShowLog);
    disposeImage(imageShowPerf);
    disposeImage(imageShowResults);
    disposeImage(imageShowSelected);
    disposeImage(imageStop);
    disposeImage(imageSynonym);
    disposeImage(imageTable);
    disposeImage(imageUndo);
    disposeImage(imageUnselectAll);
    disposeImage(imageUp);
    disposeImage(imageUser);
    disposeImage(imageView);

    // big images
    //
    disposeUniversalImages(imagesActions.values());
    disposeUniversalImages(imagesTransforms.values());

    // Dispose of the images in the map
    //
    disposeImages(imageMap.values());
    disposeImages(imagesValueMeta.values());
    disposeImages(imagesDatabase.values());
  }

  private void disposeImages(Collection<Image> c) {
    for (Image image : c) {
      disposeImage(image);
    }
  }

  private void disposeUniversalImages(Collection<SwtUniversalImage> c) {
    for (SwtUniversalImage image : c) {
      image.dispose();
    }
  }

  private void disposeImage(Image image) {
    if (image != null && !image.isDisposed()) {
      image.dispose();
    }
  }

  /** Load all transform images from files. */
  private void loadTransformImages() {
    imagesTransforms = new Hashtable<>();

    // TRANSFORM IMAGES TO LOAD
    //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> transforms = registry.getPlugins(TransformPluginType.class);
    for (IPlugin transform : transforms) {
      if (imagesTransforms.get(transform.getIds()[0]) != null) {
        continue;
      }

      SwtUniversalImage image = null;
      String filename = transform.getImageFile();
      try {
        ClassLoader classLoader = registry.getClassLoader(transform);
        image = SwtSvgImageUtil.getUniversalImage(display, classLoader, filename);
      } catch (Throwable t) {
        log.logError(
            CONST_ERROR_OCCURRED_LOADING_IMAGE + filename + CONST_FOR_PLUGIN + transform, t);
      } finally {
        if (image == null) {
          log.logError("Unable to load image file [" + filename + CONST_FOR_PLUGIN + transform);
          image = SwtSvgImageUtil.getMissingImage(display);
        }
      }

      imagesTransforms.put(transform.getIds()[0], image);
    }
  }

  private void loadFonts() {
    PropsUi props = PropsUi.getInstance();

    // We want to re-size the default font according to the global zoom factor.
    // This global zoom factor takes Hop Web (/0.75) sizing into account.
    //
    FontData defaultFontData = props.getDefaultFontData();
    int defaultFontSize =
        (int) Math.round(defaultFontData.getHeight() * props.getGlobalZoomFactor());
    defaultFontData.setHeight(defaultFontSize);
    fontDefault = new ManagedFont(display, defaultFontData);

    // The graph font needs to be smaller because it gets magnified using a zoom factor on the
    // canvas.
    //
    FontData graphFontData = props.getGraphFont();
    int graphFontSize =
        (int) Math.round(1.5 + graphFontData.getHeight() / PropsUi.getNativeZoomFactor());
    graphFontData.setHeight(graphFontSize);
    fontGraph = new ManagedFont(display, graphFontData);

    FontData noteFontData = props.getNoteFont();
    int noteFontSize = (int) Math.round(noteFontData.getHeight() * props.getGlobalZoomFactor());
    noteFontData.setHeight(noteFontSize);
    fontNote = new ManagedFont(display, noteFontData);

    FontData fixedFontData = props.getFixedFont();
    int fixedFontSize = (int) Math.round(fixedFontData.getHeight() * props.getGlobalZoomFactor());
    fixedFontData.setHeight(fixedFontSize);
    fontFixed = new ManagedFont(display, fixedFontData);

    // Create a medium size version of the graph font
    int mediumFontSize = (int) Math.round(defaultFontSize * 1.2);
    FontData mediumFontData =
        new FontData(graphFontData.getName(), mediumFontSize, graphFontData.getStyle());
    fontMedium = new ManagedFont(display, mediumFontData);

    // Create a medium bold size version of the graph font
    FontData mediumFontBoldData =
        new FontData(graphFontData.getName(), mediumFontSize, graphFontData.getStyle() | SWT.BOLD);
    fontMediumBold = new ManagedFont(display, mediumFontBoldData);

    // Create a large version of the graph font
    int largeFontSize = mediumFontSize + 2;
    FontData largeFontData =
        new FontData(graphFontData.getName(), largeFontSize, graphFontData.getStyle());
    fontLarge = new ManagedFont(display, largeFontData);

    // Create a tiny version of the graph font
    int tinyFontSize = mediumFontSize - 2;
    FontData tinyFontData =
        new FontData(graphFontData.getName(), tinyFontSize, graphFontData.getStyle());
    fontTiny = new ManagedFont(display, tinyFontData);

    // Create a small version of the graph font
    int smallFontSize = mediumFontSize - 1;
    FontData smallFontData =
        new FontData(graphFontData.getName(), smallFontSize, graphFontData.getStyle());
    fontSmall = new ManagedFont(display, smallFontData);

    FontData boldFontData =
        new FontData(
            defaultFontData.getName(),
            defaultFontData.getHeight(),
            defaultFontData.getStyle() | SWT.BOLD);
    fontBold = new ManagedFont(display, boldFontData);
  }

  // load image from svg
  //
  public Image loadAsResource(Display display, String location, int size) {
    SwtUniversalImage img =
        SwtSvgImageUtil.getUniversalImage(display, getClass().getClassLoader(), location);
    Image image;
    if (size > 0) {
      int newSize = (int) Math.round(size * zoomFactor);
      image = new Image(display, img.getAsBitmapForSize(display, newSize, newSize), SWT.IMAGE_COPY);
    } else {
      image = new Image(display, img.getAsBitmap(display), SWT.IMAGE_COPY);
    }
    img.dispose();
    return image;
  }

  // load image from svg
  public Image loadAsResource(Display display, String location, int width, int height) {
    SwtUniversalImage img = SwtSvgImageUtil.getImageAsResource(display, location);
    int newWidth = (int) Math.round(width * zoomFactor);
    int newHeight = (int) Math.round(height * zoomFactor);
    Image image =
        new Image(display, img.getAsBitmapForSize(display, newWidth, newHeight), SWT.IMAGE_COPY);
    img.dispose();
    return image;
  }

  private void loadCommonImages() {

    // Icons 16x16 for buttons, toolbar items, tree items...
    //
    imageEmpty = new Image(display, 16, 16);
    imageAdd = loadAsResource(display, "ui/images/add.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddAll = loadAsResource(display, "ui/images/add_all.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddAbove =
        loadAsResource(display, "ui/images/add-item-above.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddBelow =
        loadAsResource(display, "ui/images/add-item-below.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddSingle = loadAsResource(display, "ui/images/add_single.svg", ConstUi.SMALL_ICON_SIZE);
    imageCalendar = loadAsResource(display, "ui/images/calendar.svg", ConstUi.SMALL_ICON_SIZE);
    imageCatalog = loadAsResource(display, "ui/images/catalog.svg", ConstUi.SMALL_ICON_SIZE);
    imageCheck = loadAsResource(display, "ui/images/check.svg", ConstUi.SMALL_ICON_SIZE);
    imageCollapseAll =
        loadAsResource(display, "ui/images/collapse-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageColor = loadAsResource(display, "ui/images/color.svg", ConstUi.SMALL_ICON_SIZE);
    imageCancel = loadAsResource(display, "ui/images/cancel.svg", ConstUi.SMALL_ICON_SIZE);
    imageCopy = loadAsResource(display, "ui/images/copy.svg", ConstUi.SMALL_ICON_SIZE);
    imageCut = loadAsResource(display, "ui/images/cut.svg", ConstUi.SMALL_ICON_SIZE);
    imageDuplicate = loadAsResource(display, "ui/images/duplicate.svg", ConstUi.SMALL_ICON_SIZE);
    imagePaste = loadAsResource(display, "ui/images/paste.svg", ConstUi.SMALL_ICON_SIZE);
    imageExpandAll = loadAsResource(display, "ui/images/expand-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageLabel = loadAsResource(display, "ui/images/label.svg", ConstUi.SMALL_ICON_SIZE);
    imageFunction = loadAsResource(display, "ui/images/function.svg", ConstUi.SMALL_ICON_SIZE);
    imageKeyboard = loadAsResource(display, "ui/images/keyboard.svg", ConstUi.SMALL_ICON_SIZE);

    imageNavigateBack =
        loadAsResource(display, "ui/images/navigate-back.svg", ConstUi.SMALL_ICON_SIZE);
    imageNavigateForward =
        loadAsResource(display, "ui/images/navigate-forward.svg", ConstUi.SMALL_ICON_SIZE);
    imageNavigateUp = loadAsResource(display, "ui/images/navigate-up.svg", ConstUi.SMALL_ICON_SIZE);
    imageHelp = loadAsResource(display, "ui/images/help.svg", ConstUi.SMALL_ICON_SIZE);
    imageHide = loadAsResource(display, "ui/images/hide.svg", ConstUi.SMALL_ICON_SIZE);
    imageHideResults =
        loadAsResource(display, "ui/images/hide-results.svg", ConstUi.SMALL_ICON_SIZE);
    imageHome = loadAsResource(display, "ui/images/home.svg", ConstUi.SMALL_ICON_SIZE);
    imageMaximizePanel =
        loadAsResource(display, "ui/images/maximize-panel.svg", ConstUi.SMALL_ICON_SIZE);
    imageMinimizePanel =
        loadAsResource(display, "ui/images/minimize-panel.svg", ConstUi.SMALL_ICON_SIZE);
    imageNew = loadAsResource(display, "ui/images/new.svg", ConstUi.SMALL_ICON_SIZE);
    imageNote = loadAsResource(display, "ui/images/note.svg", ConstUi.SMALL_ICON_SIZE);
    imagePlugin = loadAsResource(display, "ui/images/plugin.svg", ConstUi.SMALL_ICON_SIZE);
    imagePrint = loadAsResource(display, "ui/images/print.svg", ConstUi.SMALL_ICON_SIZE);
    imageRefresh = loadAsResource(display, "ui/images/refresh.svg", ConstUi.SMALL_ICON_SIZE);
    imageRegex = loadAsResource(display, "ui/images/regex.svg", ConstUi.SMALL_ICON_SIZE);
    imageRemoveAll = loadAsResource(display, "ui/images/remove_all.svg", ConstUi.SMALL_ICON_SIZE);
    imageRemoveSingle =
        loadAsResource(display, "ui/images/remove_single.svg", ConstUi.SMALL_ICON_SIZE);
    imageRename = loadAsResource(display, "ui/images/rename.svg", ConstUi.SMALL_ICON_SIZE);
    imageResetOption =
        loadAsResource(display, "ui/images/reset_option.svg", ConstUi.SMALL_ICON_SIZE);
    imageRotateLeft = loadAsResource(display, "ui/images/rotate-left.svg", ConstUi.SMALL_ICON_SIZE);
    imageRotateRight =
        loadAsResource(display, "ui/images/rotate-right.svg", ConstUi.SMALL_ICON_SIZE);
    imageSchema = loadAsResource(display, "ui/images/schema.svg", ConstUi.SMALL_ICON_SIZE);
    imageSearch = loadAsResource(display, "ui/images/search.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowAll = loadAsResource(display, "ui/images/show-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowErrorLines =
        loadAsResource(display, "ui/images/show-error-lines.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowGrid = loadAsResource(display, "ui/images/show-grid.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowHistory =
        loadAsResource(display, "ui/images/show-history.svg", ConstUi.SMALL_ICON_SIZE);
    imageShow = loadAsResource(display, "ui/images/show.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowLog = loadAsResource(display, "ui/images/log.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowPerf = loadAsResource(display, "ui/images/show-perf.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowResults =
        loadAsResource(display, "ui/images/show-results.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowSelected =
        loadAsResource(display, "ui/images/show-selected.svg", ConstUi.SMALL_ICON_SIZE);
    imageSynonym = loadAsResource(display, "ui/images/view.svg", ConstUi.SMALL_ICON_SIZE);
    imageTable = loadAsResource(display, "ui/images/table.svg", ConstUi.SMALL_ICON_SIZE);
    imageUser = loadAsResource(display, "ui/images/user.svg", ConstUi.SMALL_ICON_SIZE);
    imageClose = loadAsResource(display, "ui/images/close.svg", ConstUi.SMALL_ICON_SIZE);
    imageDelete = loadAsResource(display, "ui/images/delete.svg", ConstUi.SMALL_ICON_SIZE);
    imagePause = loadAsResource(display, "ui/images/pause.svg", ConstUi.SMALL_ICON_SIZE);
    imageRun = loadAsResource(display, "ui/images/run.svg", ConstUi.SMALL_ICON_SIZE);
    imageStop = loadAsResource(display, "ui/images/stop.svg", ConstUi.SMALL_ICON_SIZE);
    imageView = loadAsResource(display, "ui/images/view.svg", ConstUi.SMALL_ICON_SIZE);
    imageDown = loadAsResource(display, "ui/images/down.svg", ConstUi.SMALL_ICON_SIZE);
    imageUp = loadAsResource(display, "ui/images/up.svg", ConstUi.SMALL_ICON_SIZE);
    imageLocation = loadAsResource(display, "ui/images/location.svg", ConstUi.SMALL_ICON_SIZE);
    imageOptions = loadAsResource(display, "ui/images/options.svg", ConstUi.SMALL_ICON_SIZE);
    imageUndo = loadAsResource(display, "ui/images/undo.svg", ConstUi.SMALL_ICON_SIZE);
    imageRedo = loadAsResource(display, "ui/images/redo.svg", ConstUi.SMALL_ICON_SIZE);
    imageClear = loadAsResource(display, "ui/images/clear.svg", ConstUi.SMALL_ICON_SIZE);
    imageSelectAll = loadAsResource(display, "ui/images/select-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageUnselectAll =
        loadAsResource(display, "ui/images/unselect-all.svg", ConstUi.SMALL_ICON_SIZE);

    // Svg image
    //
    imageLogo = SwtSvgImageUtil.getImageAsResource(display, "ui/images/logo_icon.svg");
    imagePipeline = SwtSvgImageUtil.getImageAsResource(display, "ui/images/pipeline.svg");
    imageWorkflow = SwtSvgImageUtil.getImageAsResource(display, "ui/images/workflow.svg");
    imageServer = SwtSvgImageUtil.getImageAsResource(display, "ui/images/server.svg");
    imagePreview = SwtSvgImageUtil.getImageAsResource(display, "ui/images/preview.svg");
    imageTrue = SwtSvgImageUtil.getImageAsResource(display, "ui/images/true.svg");
    imageTrueDisabled = SwtSvgImageUtil.getImageAsResource(display, "ui/images/true-disabled.svg");
    imageFalse = SwtSvgImageUtil.getImageAsResource(display, "ui/images/false.svg");
    imageFalseDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/false-disabled.svg");
    imageVariable = SwtSvgImageUtil.getImageAsResource(display, "ui/images/variable.svg");
    imageFile = SwtSvgImageUtil.getImageAsResource(display, "ui/images/file.svg");
    imageFolder = SwtSvgImageUtil.getImageAsResource(display, "ui/images/folder.svg");
    imagePartitionSchema =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/partition_schema.svg");
    imageDatabase = SwtSvgImageUtil.getImageAsResource(display, "ui/images/database.svg");
    imageData = SwtSvgImageUtil.getImageAsResource(display, "ui/images/data.svg");
    imageEdit = SwtSvgImageUtil.getImageAsResource(display, "ui/images/edit.svg");
    imageMissing = SwtSvgImageUtil.getImageAsResource(display, "ui/images/missing.svg");
    imageDeprecated = SwtSvgImageUtil.getImageAsResource(display, "ui/images/deprecated.svg");
    imageLocked = SwtSvgImageUtil.getImageAsResource(display, "ui/images/lock.svg");
    imageCopyRows = SwtSvgImageUtil.getImageAsResource(display, "ui/images/copy-rows.svg");
    imageCopyRowsDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/copy-rows-disabled.svg");
    imageFailure = SwtSvgImageUtil.getImageAsResource(display, "ui/images/failure.svg");
    imageSuccess = SwtSvgImageUtil.getImageAsResource(display, "ui/images/success.svg");
    imageError = SwtSvgImageUtil.getImageAsResource(display, "ui/images/error.svg");
    imageErrorDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/error-disabled.svg");
    imageInfo = SwtSvgImageUtil.getImageAsResource(display, "ui/images/info.svg");
    imageInfoDisabled = SwtSvgImageUtil.getImageAsResource(display, "ui/images/info-disabled.svg");
    imageWarning = SwtSvgImageUtil.getImageAsResource(display, "ui/images/warning.svg");
    imageEdit = SwtSvgImageUtil.getImageAsResource(display, "ui/images/edit.svg");
    imageInput = SwtSvgImageUtil.getImageAsResource(display, "ui/images/input.svg");
    imageOutput = SwtSvgImageUtil.getImageAsResource(display, "ui/images/output.svg");
    imageTarget = SwtSvgImageUtil.getImageAsResource(display, "ui/images/target.svg");
    imageTargetDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/target-disabled.svg");
    imageParallel = SwtSvgImageUtil.getImageAsResource(display, "ui/images/parallel-hop.svg");
    imageParallelDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/parallel-hop-disabled.svg");
    imageUnconditional = SwtSvgImageUtil.getImageAsResource(display, "ui/images/unconditional.svg");
    imageUnconditionalDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/unconditional-disabled.svg");
    imageBusy = SwtSvgImageUtil.getImageAsResource(display, "ui/images/busy.svg");
    imageWaiting = SwtSvgImageUtil.getImageAsResource(display, "ui/images/waiting.svg");
    imageInject = SwtSvgImageUtil.getImageAsResource(display, "ui/images/inject.svg");
    imageBalance = SwtSvgImageUtil.getImageAsResource(display, "ui/images/scales.svg");
    imageCheckpoint = SwtSvgImageUtil.getImageAsResource(display, "ui/images/checkpoint.svg");

    // Hop arrow
    //
    imageArrowDefault =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-default.svg");
    imageArrowTrue = SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-true.svg");
    imageArrowFalse = SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-false.svg");
    imageArrowError = SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-error.svg");
    imageArrowDisabled =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-disabled.svg");
    imageArrowCandidate =
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/hop-arrow-candidate.svg");
  }

  /** Load the plugin image from a file. */
  private Image loadPluginImage(IPlugin plugin, Image defaultImage) {
    // If no image defined, use default image
    if (Utils.isEmpty(plugin.getImageFile())) {
      return defaultImage;
    }

    Image image = null;
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      ClassLoader classLoader = registry.getClassLoader(plugin);
      image =
          getImage(
              plugin.getImageFile(), classLoader, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
    } catch (Throwable t) {
      log.logError(
          CONST_ERROR_OCCURRED_LOADING_IMAGE
              + plugin.getImageFile()
              + CONST_FOR_PLUGIN
              + plugin.getIds()[0],
          t);
    } finally {
      if (image == null) {
        log.logError(
            "Unable to load image ["
                + plugin.getImageFile()
                + CONST_FOR_PLUGIN
                + plugin.getIds()[0]);
        image = defaultImage;
      }
    }

    return image;
  }

  /** Load all action images from files. */
  private void loadActionImages() {
    imagesActions = new Hashtable<>();

    // ACTION IMAGES TO LOAD
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(ActionPluginType.class);
    for (IPlugin plugin : plugins) {
      SwtUniversalImage image = null;
      String filename = plugin.getImageFile();
      try {
        ClassLoader classLoader = registry.getClassLoader(plugin);
        image = SwtSvgImageUtil.getUniversalImage(display, classLoader, filename);
      } catch (Throwable t) {
        log.logError(
            CONST_ERROR_OCCURRED_LOADING_IMAGE + filename + CONST_FOR_PLUGIN + plugin.getIds()[0],
            t);
      } finally {
        if (image == null) {
          log.logError("Unable to load image [" + filename + CONST_FOR_PLUGIN + plugin.getIds()[0]);
          image = SwtSvgImageUtil.getMissingImage(display);
        }
      }

      imagesActions.put(plugin.getIds()[0], image);
    }
  }

  /** Load all IDatabase images from files. */
  private void loadDatabaseImages() {
    imagesDatabase = new HashMap<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(DatabasePluginType.class);
    for (IPlugin plugin : plugins) {
      Image image = this.loadPluginImage(plugin, getImageDatabase());
      imagesDatabase.put(plugin.getIds()[0], image);
    }
  }

  /** Load all IValueMeta images from files. */
  private void loadValueMetaImages() {
    imagesValueMeta = new HashMap<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(ValueMetaPluginType.class);
    for (IPlugin plugin : plugins) {
      Image image = this.loadPluginImage(plugin, imageLabel);
      imagesValueMeta.put(plugin.getIds()[0], image);
    }
  }

  /**
   * @return Returns the fontFixed.
   */
  public Font getFontFixed() {
    return fontFixed.getFont();
  }

  /**
   * @return Returns the fontGraph.
   */
  public Font getFontGraph() {
    return fontGraph.getFont();
  }

  /**
   * @return Returns the default system font size adjusted for the global zoom factor.
   */
  public Font getFontDefault() {
    return fontDefault.getFont();
  }

  /**
   * @return Returns the fontNote.
   */
  public Font getFontNote() {
    return fontNote.getFont();
  }

  /**
   * Use the image folder instead.
   *
   * @return Returns the imageBol.
   */
  @Deprecated
  public Image getImageBol() {
    return imageFolder.getAsBitmapForSize(
        display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return Returns the imageServer.
   */
  public Image getImageServer() {
    return imageServer.getAsBitmapForSize(
        display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return Returns the database image.
   */
  public Image getImageDatabase() {
    return imageDatabase.getAsBitmapForSize(
        display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return Returns the data image.
   */
  public Image getImageData() {
    return imageData.getAsBitmapForSize(display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageDatabase() {
    return imageDatabase;
  }

  public SwtUniversalImage getSwtImageData() {
    return imageData;
  }

  /**
   * @return Returns the preview image.
   */
  public Image getImagePreview() {
    return imagePreview.getAsBitmapForSize(
        display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return Returns the imageMissing.
   */
  public Image getImageMissing() {
    return imageMissing.getAsBitmapForSize(display, ConstUi.ICON_SIZE, ConstUi.ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageMissing() {
    return imageMissing;
  }

  /**
   * @return Returns the imageHop.
   */
  public Image getImageHop() {
    return imageLogo.getAsBitmapForSize(display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * Get an image of an action or a missing image if not found.
   *
   * @param pluginId the action plugin id
   * @return image
   */
  public SwtUniversalImage getSwtImageAction(String pluginId) {
    SwtUniversalImage image = imagesActions.get(pluginId);
    if (image == null) {
      return getSwtImageMissing();
    }
    return image;
  }

  /**
   * Get an image of a transform or a missing image if not found.
   *
   * @param pluginId the transform plugin id
   * @return image
   */
  public SwtUniversalImage getSwtImageTransform(String pluginId) {
    SwtUniversalImage image = imagesTransforms.get(pluginId);
    if (image == null) {
      return getSwtImageMissing();
    }
    return image;
  }

  /**
   * Return the image of the IValueMeta from plugin
   *
   * @return image
   */
  public Image getImage(IValueMeta valueMeta) {
    if (valueMeta == null) return this.imageLabel;
    return imagesValueMeta.get(String.valueOf(valueMeta.getType()));
  }

  /**
   * Return the image of the IDatabase from plugin
   *
   * @return image
   */
  public Image getImage(IDatabase database) {
    if (database == null) return this.getImageDatabase();
    return imagesDatabase.get(database.getPluginId());
  }

  /**
   * @return the fontLarge
   */
  public Font getFontLarge() {
    return fontLarge.getFont();
  }

  /**
   * @return the tiny font
   */
  public Font getFontTiny() {
    return fontTiny.getFont();
  }

  /**
   * @return the small font
   */
  public Font getFontSmall() {
    return fontSmall.getFont();
  }

  /**
   * @return Returns the clipboard.
   */
  public Clipboard getNewClipboard() {
    if (clipboard != null) {
      clipboard.dispose();
      clipboard = null;
    }
    clipboard = new Clipboard(display);

    return clipboard;
  }

  public void toClipboard(String cliptext) {
    if (cliptext == null) {
      return;
    }

    getNewClipboard();
    TextTransfer tran = TextTransfer.getInstance();
    clipboard.setContents(new String[] {cliptext}, new Transfer[] {tran});
  }

  public String fromClipboard() {
    getNewClipboard();
    TextTransfer tran = TextTransfer.getInstance();

    return (String) clipboard.getContents(tran);
  }

  public Font getFontBold() {
    return fontBold.getFont();
  }

  private Image getZoomedImaged(
      SwtUniversalImage universalImage, Device device, int width, int height) {
    return universalImage.getAsBitmapForSize(
        device, (int) (zoomFactor * width), (int) (zoomFactor * height));
  }

  /**
   * @return the imageVariable
   */
  public Image getImageVariable() {
    return getZoomedImaged(
        imageVariable, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public Image getImageVariableMini() {
    return getZoomedImaged(imageVariable, display, 12, 12);
  }

  public Image getImagePipeline() {
    return getZoomedImaged(
        imagePipeline, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  @Deprecated
  public Image getImageClosePanel() {
    return imageClose;
  }

  @Deprecated
  public Image getImageFolderConnections() {
    return getZoomedImaged(
        imagePipeline, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public Image getImagePartitionSchema() {
    return getZoomedImaged(
        imagePartitionSchema, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE);
  }

  public Image getImageWorkflow() {
    return getZoomedImaged(
        imageWorkflow, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return the imageArrow
   */
  public Image getImageFolder() {
    return getZoomedImaged(imageFolder, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return the imageFile
   */
  public Image getImageFile() {
    return getZoomedImaged(imageFile, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return the imageLogoSmall
   */
  public Image getImageHopUi() {
    return getZoomedImaged(imageLogo, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * @return the image size for Taskbar
   */
  public Image getImageHopUiTaskbar() {
    if (OsHelper.isMac()) {
      return getZoomedImaged(imageLogo, display, 512, 512);
    } else {
      return getZoomedImaged(imageLogo, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
    }
  }

  public void drawGradient(Display display, GC gc, Rectangle rect, boolean vertical) {
    if (!vertical) {
      gc.setForeground(display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
      gc.setBackground(GuiResource.getInstance().getColorHop());
      gc.fillGradientRectangle(rect.x, rect.y, 2 * rect.width / 3, rect.height, vertical);
      gc.setForeground(GuiResource.getInstance().getColorHop());
      gc.setBackground(display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
      gc.fillGradientRectangle(
          rect.x + 2 * rect.width / 3, rect.y, rect.width / 3 + 1, rect.height, vertical);
    } else {
      gc.setForeground(display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
      gc.setBackground(GuiResource.getInstance().getColorHop());
      gc.fillGradientRectangle(rect.x, rect.y, rect.width, 2 * rect.height / 3, vertical);
      gc.setForeground(GuiResource.getInstance().getColorHop());
      gc.setBackground(display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
      gc.fillGradientRectangle(
          rect.x, rect.y + 2 * rect.height / 3, rect.width, rect.height / 3 + 1, vertical);
    }
  }

  public static Point calculateControlPosition(Control control) {
    // Calculate the exact location...
    //
    Rectangle r = control.getBounds();
    return control.getParent().toDisplay(r.x, r.y);
  }

  /**
   * @return the fontMedium
   */
  public Font getFontMedium() {
    return fontMedium.getFont();
  }

  /**
   * @return the fontMediumBold
   */
  public Font getFontMediumBold() {
    return fontMediumBold.getFont();
  }

  public Image getImageCopyHop() {
    return getZoomedImaged(
        imageCopyRows, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageCopyRows() {
    return imageCopyRows;
  }

  public SwtUniversalImage getSwtImageCopyRowsDisabled() {
    return imageCopyRowsDisabled;
  }

  public Image getImageError() {
    return getZoomedImaged(imageError, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageError() {
    return imageError;
  }

  public SwtUniversalImage getSwtImageErrorDisabled() {
    return imageErrorDisabled;
  }

  public Image getImageInfo() {
    return getZoomedImaged(imageInfo, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageInfo() {
    return imageInfo;
  }

  public SwtUniversalImage getSwtImageInfoDisabled() {
    return imageInfoDisabled;
  }

  public Image getImageWarning() {
    return getZoomedImaged(imageWarning, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageWarning() {
    return imageWarning;
  }

  public SwtUniversalImage getSwtImageDeprecated() {
    return imageDeprecated;
  }

  public Image getImageDeprecated() {
    return getZoomedImaged(
        imageDeprecated, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public Image getImageEdit() {
    return getZoomedImaged(imageEdit, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageEdit() {
    return imageEdit;
  }

  public Image getImageInput() {
    return getZoomedImaged(imageInput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageInput() {
    return imageInput;
  }

  public Image getImageOutput() {
    return getZoomedImaged(imageOutput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageOutput() {
    return imageOutput;
  }

  public Image getImageTarget() {
    return getZoomedImaged(imageTarget, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageTarget() {
    return imageTarget;
  }

  public SwtUniversalImage getSwtImageTargetDisabled() {
    return imageTargetDisabled;
  }

  public Image getImageLocked() {
    return getZoomedImaged(imageLocked, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageLocked() {
    return imageLocked;
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of
   * this, it's important to never dispose of the image you get from here. (easy!) The images are
   * automatically disposed when the application ends.
   *
   * @param location the location of the image resource to load
   * @return the loaded image
   */
  public Image getImage(String location) {
    return getImage(location, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of
   * this, it's important to never dispose of the image you get from here. (easy!) The images are
   * automatically disposed when the application ends.
   *
   * @param location the location of the image resource to load
   * @param width The height to resize the image to
   * @param height The width to resize the image to
   * @return the loaded image
   */
  public Image getImage(String location, int width, int height) {
    StringBuilder builder = new StringBuilder(location);
    builder.append('|');
    builder.append(width);
    builder.append('|');
    builder.append(height);
    String key = builder.toString();

    Image image = imageMap.get(key);
    if (image == null) {
      SwtUniversalImage svg = SwtSvgImageUtil.getImage(display, location);
      int realWidth = (int) Math.round(zoomFactor * width);
      int realHeight = (int) Math.round(zoomFactor * height);
      image =
          new Image(
              display, svg.getAsBitmapForSize(display, realWidth, realHeight), SWT.IMAGE_COPY);
      svg.dispose();
      imageMap.put(key, image);
    }
    return image;
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of
   * this, it's important to never dispose of the image you get from here. (easy!) The images are
   * automatically disposed when the application ends.
   *
   * @param location the location of the image resource to load
   * @param classLoader the ClassLoader to use to locate resources
   * @param width The height to resize the image to
   * @param height The width to resize the image to
   * @return the loaded image
   */
  public Image getImage(String location, ClassLoader classLoader, int width, int height) {
    return getImage(location, classLoader, width, height, false);
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of
   * this, it's important to never dispose of the image you get from here. (easy!) The images are
   * automatically disposed when the application ends.
   *
   * @param location the location of the image resource to load
   * @param classLoader the ClassLoader to use to locate resources
   * @param width The height to resize the image to
   * @param height The width to resize the image to
   * @param disabled in case you want to gray-scaled 'disabled' version of the image
   * @return the loaded image
   */
  public Image getImage(
      String location, ClassLoader classLoader, int width, int height, boolean disabled) {
    // Build image key for a specific size
    StringBuilder builder = new StringBuilder(location);
    builder.append('|').append(width).append('|').append(height).append('|').append(disabled);
    String key = builder.toString();

    Image image = imageMap.get(key);
    if (image == null) {
      SwtUniversalImage svg = SwtSvgImageUtil.getUniversalImage(display, classLoader, location);

      Image zoomedImaged = getZoomedImaged(svg, display, width, height);
      if (disabled) {
        // First disabled the image...
        //
        image = new Image(display, zoomedImaged, SWT.IMAGE_GRAY);

        // Now darken or lighten the image...
        //
        float factor;
        if (PropsUi.getInstance().isDarkMode()) {
          factor = 0.4f;
        } else {
          factor = 2.5f;
        }

        ImageData data = image.getImageData();
        for (int x = 0; x < data.width; x++) {
          for (int y = 0; y < data.height; y++) {
            int pixel = data.getPixel(x, y);
            int a = (pixel >> 24) & 0xFF;
            int b = (pixel >> 16) & 0xFF;
            int g = (pixel >> 8) & 0xFF;
            int r = pixel & 0xFF;
            a = (int) (a * factor);
            b = (int) (b * factor);
            g = (int) (g * factor);
            r = (int) (r * factor);
            data.setPixel(x, y, r + (g << 8) + (b << 16) + (a << 25));
          }
          image.dispose();
          image = new Image(display, data);
        }
      } else {
        image = new Image(display, zoomedImaged, SWT.IMAGE_COPY);
      }

      svg.dispose();
      imageMap.put(key, image);
    }
    return image;
  }

  public Color getColor(int red, int green, int blue) {
    RGB rgb = new RGB(red, green, blue);
    Color color = colorMap.get(rgb);
    if (color == null) {
      color = new Color(display, rgb);
      colorMap.put(rgb, color);
    }
    return color;
  }

  /**
   * @return the imageTrue
   */
  public Image getImageTrue() {
    return getZoomedImaged(imageTrue, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageTrue() {
    return imageTrue;
  }

  public SwtUniversalImage getSwtImageTrueDisabled() {
    return imageTrueDisabled;
  }

  /**
   * @return the imageFalse
   */
  public Image getImageFalse() {
    return getZoomedImaged(imageFalse, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageFalse() {
    return imageFalse;
  }

  public SwtUniversalImage getSwtImageFalseDisabled() {
    return imageFalseDisabled;
  }

  public Image getImageFailure() {
    return getZoomedImaged(imageFailure, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageFailure() {
    return imageFailure;
  }

  public Image getImageSuccess() {
    return getZoomedImaged(imageSuccess, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageSuccess() {
    return imageSuccess;
  }

  public Image getImageParallelHop() {
    return getZoomedImaged(
        imageParallel, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageParallel() {
    return imageParallel;
  }

  public SwtUniversalImage getSwtImageParallelDisabled() {
    return imageParallelDisabled;
  }

  public Image getImageUnconditionalHop() {
    return getZoomedImaged(
        imageUnconditional, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageUnconditional() {
    return imageUnconditional;
  }

  public SwtUniversalImage getSwtImageUnconditionalDisabled() {
    return imageUnconditionalDisabled;
  }

  public Image getImageBusy() {
    return getZoomedImaged(imageBusy, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageBusy() {
    return imageBusy;
  }

  public SwtUniversalImage getSwtImageWaiting() {
    return imageWaiting;
  }

  public Image getImageInject() {
    return getZoomedImaged(imageInject, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageInject() {
    return imageInject;
  }

  public Image getImageBalance() {
    return getZoomedImaged(imageBalance, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageBalance() {
    return imageBalance;
  }

  public Image getImageCheckpoint() {
    return getZoomedImaged(
        imageCheckpoint, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
  }

  public SwtUniversalImage getSwtImageCheckpoint() {
    return imageCheckpoint;
  }

  public SwtUniversalImage getSwtImageArrowDefault() {
    return imageArrowDefault;
  }

  public SwtUniversalImage getSwtImageArrowTrue() {
    return imageArrowTrue;
  }

  public SwtUniversalImage getSwtImageArrowFalse() {
    return imageArrowFalse;
  }

  public SwtUniversalImage getSwtImageArrowError() {
    return imageArrowError;
  }

  public SwtUniversalImage getSwtImageArrowDisabled() {
    return imageArrowDisabled;
  }

  public SwtUniversalImage getSwtImageArrowCandidate() {
    return imageArrowCandidate;
  }

  public Color getWidgetBackGroundColor() {
    if (OsHelper.isMac()) {
      if (PropsUi.getInstance().isDarkMode()) {
        return display.getSystemColor(SWT.COLOR_WIDGET_BACKGROUND);
      } else {
        return colorDemoGray;
      }
    } else {
      return colorWhite;
    }
  }
}
