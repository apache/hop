//CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.ui.core.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
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
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/*
 * colors etc. are allocated once and released once at the end of the program.
 *
 * @author Matt
 * @since 27/10/2005
 *
 */
public class GuiResource {

  private static ILogChannel log = LogChannel.UI;

  private static GuiResource guiResource;

  private Display display;

  private double zoomFactor;

  // 33 resources

  /* * * Colors * * */
  private ManagedColor colorBackground;

  private ManagedColor colorGraph;

  private ManagedColor colorTab;

  private ManagedColor colorRed;

  private ManagedColor colorSuccessGreen;

  private ManagedColor colorBlueCustomGrid;

  private ManagedColor colorGreen;

  private ManagedColor colorBlue;

  private ManagedColor colorOrange;

  private ManagedColor colorYellow;

  private ManagedColor colorMagenta;

  private ManagedColor colorBlack;

  private ManagedColor colorGray;

  private ManagedColor colorDarkGray;

  private ManagedColor colorLightGray;

  private ManagedColor colorDemoGray;

  private ManagedColor colorWhite;

  private ManagedColor colorDirectory;

  private ManagedColor colorHop;

  private ManagedColor colorLight;

  private ManagedColor colorCream;

  private ManagedColor colorLightBlue;

  private ManagedColor colorCrystalText;

  private ManagedColor colorHopDefault;

  private ManagedColor colorHopOK;

  private ManagedColor colorDeprecated;

  /* * * Fonts * * */
  private ManagedFont fontGraph;

  private ManagedFont fontNote;

  private ManagedFont fontFixed;

  private ManagedFont fontMedium;

  private ManagedFont fontMediumBold;

  private ManagedFont fontLarge;

  private ManagedFont fontTiny;

  private ManagedFont fontSmall;

  private ManagedFont fontBold;

  /* * * Images * * */
  private Map<String, SwtUniversalImage> imagesTransforms = new Hashtable<>();

  private Map<String, SwtUniversalImage> imagesActions;

  private Map<String, Image> imagesValueMeta;
  

  private SwtUniversalImage imageLogo;
  private SwtUniversalImage imageDisabledHop;
  private SwtUniversalImage imageConnection;
  private SwtUniversalImage imageData;
  private SwtUniversalImage imagePreview;
  private SwtUniversalImage imageMissing;
  private SwtUniversalImage imageDeprecated;
  private SwtUniversalImage imageVariable;
  private SwtUniversalImage imagePipeline;
  private SwtUniversalImage imagePartitionSchema;
  private SwtUniversalImage imageWorkflow;
  private SwtUniversalImage imageArrowDefault;
  private SwtUniversalImage imageArrowOk;
  private SwtUniversalImage imageArrowError;
  private SwtUniversalImage imageArrowDisabled;
  private SwtUniversalImage imageArrowCandidate;
  private SwtUniversalImage imageBol;
  private SwtUniversalImage imageServer;
  private SwtUniversalImage imageArrow;
  private SwtUniversalImage imageFolder;
  private SwtUniversalImage imageFile;
  private SwtUniversalImage imageFolderConnections;
  private SwtUniversalImage imageEdit;
  private SwtUniversalImage imageClearText;
  private SwtUniversalImage imageTransformError;
  private SwtUniversalImage imageRedTransformError;
  private SwtUniversalImage imageCopyRows;
  private SwtUniversalImage imageError;
  private SwtUniversalImage imageInfo;
  private SwtUniversalImage imageWarning;
  private SwtUniversalImage imageInput;
  private SwtUniversalImage imageOutput;
  private SwtUniversalImage imageTarget;
  private SwtUniversalImage imageLocked;
  private SwtUniversalImage imageTrue;
  private SwtUniversalImage imageFalse;
  private SwtUniversalImage imageContextMenu;
  private SwtUniversalImage imageUnconditionalHop;
  private SwtUniversalImage imageParallelHop;
  private SwtUniversalImage imageBusy;
  private SwtUniversalImage imageInject;
  private SwtUniversalImage imageBalance;
  private SwtUniversalImage imageCheckpoint;
  
  private Image imageEmpty;
  private Image imageExpandAll;
  private Image imageCollapseAll;
  private Image imageAdd;
  private Image imageCopy;
  private Image imageCancel;
  private Image imageCut;
  private Image imageDuplicate;
  private Image imagePaste;
  private Image imageTable;
  private Image imageSchema;
  private Image imageSynonym;
  private Image imageView;
  private Image imageCalendar;
  private Image imageLabel;
  private Image imageFunction;
  private Image imageUser;
  private Image imageEditOption;
  private Image imageColor;
  private Image imageNote;
  private Image imageResetOption;
  private Image imageShowLog;
  private Image imageShowGrid;
  private Image imageShowHistory;
  private Image imageShowPerf;
  private Image imageShow;
  private Image imageHide;
  private Image imageShowSelected;
  private Image imageShowAll;
  private Image imageClosePanel;
  private Image imageMaximizePanel;
  private Image imageMinimizePanel;
  private Image imageShowErrorLines;
  private Image imageShowResults;
  private Image imageHideResults;
  private Image imageSearch;
  private Image imageRegEx;  
  private Image imageAddAll;
  private Image imageAddSingle;
  private Image imageRemoveAll;
  private Image imageRemoveSingle;
  private Image imageBack;
  private Image imageForward;
  private Image imageRefresh;
  private Image imageHome;
  private Image imagePrint;
  private Image imageHelp;
  private Image imageCleanup;
  private Image imageClose;
  private Image imageDelete;
  private Image imagePause;
  private Image imageRun;
  private Image imageStop;
  private Image imageNew;
 

  private Map<String, Image> imageMap;

  private Map<RGB, Color> colorMap;

  /**
   * GuiResource also contains the clipboard as it has to be allocated only once! I don't want to put it in a separate
   * singleton just for this one member.
   */
  private static Clipboard clipboard;

  private GuiResource( Display display ) {
    this.display = display;

    getResources();

    display.addListener( SWT.Dispose, event -> dispose( false ) );

    clipboard = null;

    // Reload images as required by changes in the plugins
    PluginRegistry.getInstance().addPluginListener( TransformPluginType.class, new IPluginTypeListener() {
      @Override
      public void pluginAdded( Object serviceObject ) {
        loadTransformImages();
      }

      @Override
      public void pluginRemoved( Object serviceObject ) {
        loadTransformImages();
      }

      @Override
      public void pluginChanged( Object serviceObject ) {
      }
    } );

    PluginRegistry.getInstance().addPluginListener( ActionPluginType.class, new IPluginTypeListener() {
      @Override public void pluginAdded( Object serviceObject ) {
        // make sure we load up the images for any new actions that have been registered
        loadWorkflowActionImages();
      }

      @Override public void pluginRemoved( Object serviceObject ) {
        // rebuild the image map, in effect removing the image(s) for actions that have gone away
        loadWorkflowActionImages();
      }

      @Override public void pluginChanged( Object serviceObject ) {
        // nothing needed here
      }
    } );

  }

  public static final GuiResource getInstance() {
    if ( guiResource != null ) {
      return guiResource;
    }
    guiResource = new GuiResource( Display.getCurrent() );
    return guiResource;
  }

  /**
   * reloads all colors, fonts and images.
   */
  public void reload() {
    dispose( true );
    getResources();
  }

  private void getResources() {
    PropsUi props = PropsUi.getInstance();
    zoomFactor = props.getZoomFactor();
    imageMap = new HashMap<>();
    colorMap = new HashMap<>();

    colorBackground = new ManagedColor( display, props.getBackgroundRGB() );
    colorGraph = new ManagedColor( display, props.getGraphColorRGB() );
    colorTab = new ManagedColor( display, props.getTabColorRGB() );
    colorSuccessGreen = new ManagedColor( display, 0, 139, 0 );
    colorRed = new ManagedColor( display, 255, 0, 0 );
    colorGreen = new ManagedColor( display, 0, 255, 0 );
    colorBlue = new ManagedColor( display, 0, 0, 255 );
    colorYellow = new ManagedColor( display, 255, 255, 0 );
    colorMagenta = new ManagedColor( display, 255, 0, 255 );
    colorOrange = new ManagedColor( display, 255, 165, 0 );

    colorBlueCustomGrid = new ManagedColor( display, 240, 248, 255 );

    colorWhite = new ManagedColor( display, 255, 255, 255 );
    colorDemoGray = new ManagedColor( display, 240, 240, 240 );
    colorLightGray = new ManagedColor( display, 225, 225, 225 );
    colorGray = new ManagedColor( display, 215, 215, 215 );
    colorDarkGray = new ManagedColor( display, 100, 100, 100 );
    colorBlack = new ManagedColor( display, 0, 0, 0 );
    colorLightBlue = new ManagedColor( display, 135, 206, 250 ); // light sky blue

    colorDirectory = new ManagedColor( display, 0, 0, 255 );
    // colorHop = new ManagedColor(display, 239, 128, 51 ); // Orange
    colorHop = new ManagedColor( display, 188, 198, 82 );
    colorLight = new ManagedColor( display, 238, 248, 152 );
    colorCream = new ManagedColor( display, 248, 246, 231 );

    colorCrystalText = new ManagedColor( display, 61, 99, 128 );

    colorHopDefault = new ManagedColor( display, 61, 99, 128 );

    colorHopOK = new ManagedColor( display, 12, 178, 15 );

    colorDeprecated = new ManagedColor( display, 246, 196, 56 );
    // Load all images from files...
    loadFonts();
    loadCommonImages();
    loadTransformImages();
    loadWorkflowActionImages();
    loadValueMetaImages();
  }

  private void dispose( boolean reload ) {
    // Colors
    colorBackground.dispose();
    colorGraph.dispose();
    colorTab.dispose();

    colorRed.dispose();
    colorSuccessGreen.dispose();
    colorGreen.dispose();
    colorBlue.dispose();
    colorGray.dispose();
    colorYellow.dispose();
    colorMagenta.dispose();
    colorOrange.dispose();
    colorBlueCustomGrid.dispose();

    colorWhite.dispose();
    colorDemoGray.dispose();
    colorLightGray.dispose();
    colorDarkGray.dispose();
    colorBlack.dispose();
    colorLightBlue.dispose();

    colorDirectory.dispose();
    colorHop.dispose();
    colorLight.dispose();
    colorCream.dispose();

    disposeColors( colorMap.values() );

    if ( !reload ) {
      // display shutdown, clean up our mess

      // Fonts
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
      imageDisabledHop.dispose();
      imageConnection.dispose();
      imageData.dispose();
      imageAdd.dispose();
      imageTable.dispose();
      imagePreview.dispose();
      imageSchema.dispose();
      imageSynonym.dispose();
      imageView.dispose();
      imageLabel.dispose();
      imageFunction.dispose();
      imageCancel.dispose();
      imageCopy.dispose();
      imageCut.dispose();
      imageDuplicate.dispose();
      imagePaste.dispose();
      imageBol.dispose();
      imageCalendar.dispose();
      imageServer.dispose();
      imageArrow.dispose();
      imageFile.dispose();
      imageFolder.dispose();
      imageMissing.dispose();
      imageVariable.dispose();
      imagePipeline.dispose();
      imagePartitionSchema.dispose();
      imageWorkflow.dispose();
      imageUser.dispose();
      imageFolderConnections.dispose();
      imageShowResults.dispose();
      imageHideResults.dispose();
      imageCollapseAll.dispose();
      imageTransformError.dispose();
      imageRedTransformError.dispose();
      imageCopyRows.dispose();
      imageError.dispose();
      imageInfo.dispose();
      imageWarning.dispose();
      imageClearText.dispose();
      imageDeprecated.dispose();
      imageExpandAll.dispose();
      imageSearch.dispose();
      imageRegEx.dispose();
      imageNew.dispose();
      imageEdit.dispose();
      imageLocked.dispose();
      imageInput.dispose();
      imageOutput.dispose();
      imageTarget.dispose();
      imageTrue.dispose();
      imageFalse.dispose();
      imageContextMenu.dispose();
      imageParallelHop.dispose();
      imageUnconditionalHop.dispose();
      imageBusy.dispose();
      imageInject.dispose();
      imageBalance.dispose();
      imageCheckpoint.dispose();
      imageHelp.dispose();
      imageAddAll.dispose();
      imageAddSingle.dispose();
      imageRemoveAll.dispose();
      imageRemoveSingle.dispose();
      imageBack.dispose();
      imageForward.dispose();
      imageRefresh.dispose();
      imageHome.dispose();
      imagePrint.dispose();
      imageCleanup.dispose();
      imageClose.dispose();
      imageDelete.dispose();
      imagePause.dispose();
      imageRun.dispose();
      imageStop.dispose();
      imageSearch.dispose();    
      
      imageArrowDefault.dispose();
      imageArrowOk.dispose();
      imageArrowError.dispose();
      imageArrowDisabled.dispose();
      imageArrowCandidate.dispose();

      disposeImage( imageNote );
      disposeImage( imageColor );
      disposeImage( imageEditOption );
      disposeImage( imageResetOption );
      disposeImage( imageShowLog );
      disposeImage( imageShowGrid );
      disposeImage( imageShowHistory );
      disposeImage( imageShowPerf );
      disposeImage( imageShow );
      disposeImage( imageHide );
      disposeImage( imageShowSelected );
      disposeImage( imageShowAll );
      disposeImage( imageClosePanel );
      disposeImage( imageMaximizePanel );
      disposeImage( imageMinimizePanel );
      disposeImage( imageShowErrorLines );



      // big images
      disposeUniversalImages( imagesActions.values() );
      disposeUniversalImages( imagesTransforms.values() );

      // Dispose of the images in the map
      disposeImages( imageMap.values() );
      
      disposeImages (imagesValueMeta.values() );
    }
  }

  private void disposeImages( Collection<Image> c ) {
    for ( Image image : c ) {
      disposeImage( image );
    }
  }

  private void disposeUniversalImages( Collection<SwtUniversalImage> c ) {
    for ( SwtUniversalImage image : c ) {
      image.dispose();
    }
  }

  private void disposeColors( Collection<Color> colors ) {
    for ( Color color : colors ) {
      color.dispose();
    }
  }

  private void disposeImage( Image image ) {
    if ( image != null && !image.isDisposed() ) {
      image.dispose();
    }
  }

  /**
   * Load all transform images from files.
   */
  private void loadTransformImages() {
    imagesTransforms.clear();

    // TRANSFORM IMAGES TO LOAD
    //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> transforms = registry.getPlugins( TransformPluginType.class );
    for ( IPlugin transform : transforms ) {
      if ( imagesTransforms.get( transform.getIds()[ 0 ] ) != null ) {
        continue;
      }

      SwtUniversalImage image = null;
      String filename = transform.getImageFile();
      try {
        ClassLoader classLoader = registry.getClassLoader( transform );
        image = SwtSvgImageUtil.getUniversalImage( display, classLoader, filename );
      } catch ( Throwable t ) {
        log.logError( "Error occurred loading image [" + filename + "] for plugin " + transform, t );
      } finally {
        if ( image == null ) {
          log.logError( "Unable to load image file [" + filename + "] for plugin " + transform );
          image = SwtSvgImageUtil.getMissingImage( display );
        }
      }

      imagesTransforms.put( transform.getIds()[ 0 ], image );
    }
  }

  private void loadFonts() {
    PropsUi props = PropsUi.getInstance();

    fontGraph = new ManagedFont( display, props.getGraphFont() );
    fontNote = new ManagedFont( display, props.getNoteFont() );
    fontFixed = new ManagedFont( display, props.getFixedFont() );

    // Create a medium size version of the graph font
    FontData mediumFontData = new FontData( props.getGraphFont().getName(), (int) Math.round( props.getGraphFont().getHeight() * 1.2 ), props.getGraphFont().getStyle() );
    fontMedium = new ManagedFont( display, mediumFontData );

    // Create a medium bold size version of the graph font
    FontData mediumFontBoldData = new FontData( props.getGraphFont().getName(), (int) Math.round( props.getGraphFont().getHeight() * 1.2 ), props.getGraphFont().getStyle() | SWT.BOLD );
    fontMediumBold = new ManagedFont( display, mediumFontBoldData );

    // Create a large version of the graph font
    FontData largeFontData =new FontData( props.getGraphFont().getName(), mediumFontData.getHeight() + 2, props.getGraphFont().getStyle() );
    fontLarge = new ManagedFont( display, largeFontData );

    // Create a tiny version of the graph font
    FontData tinyFontData = new FontData( props.getGraphFont().getName(), mediumFontData.getHeight() - 2, props.getGraphFont().getStyle() );
    fontTiny = new ManagedFont( display, tinyFontData );

    // Create a small version of the graph font
    FontData smallFontData = new FontData( props.getGraphFont().getName(), mediumFontData.getHeight() - 1, props.getGraphFont().getStyle() );
    fontSmall = new ManagedFont( display, smallFontData );

    FontData boldFontData = new FontData( props.getDefaultFontData().getName(), props.getDefaultFontData().getHeight(), props.getDefaultFontData().getStyle() | SWT.BOLD );
    fontBold = new ManagedFont( display, boldFontData );
  }

  // load image from svg
  //
  public Image loadAsResource( Display display, String location, int size ) {
    SwtUniversalImage img = SwtSvgImageUtil.getImageAsResource( display, location );
    Image image;
    if ( size > 0 ) {
      int newSize = (int) Math.round( size * zoomFactor );
      image = new Image( display, img.getAsBitmapForSize( display, newSize, newSize ), SWT.IMAGE_COPY );
    } else {
      image = new Image( display, img.getAsBitmap( display ), SWT.IMAGE_COPY );
    }
    img.dispose();
    return image;
  }

  // load image from svg
  public Image loadAsResource( Display display, String location, int width, int height ) {
    SwtUniversalImage img = SwtSvgImageUtil.getImageAsResource( display, location );
    int newWidth = (int) Math.round( width * zoomFactor );
    int newHeight = (int) Math.round( height * zoomFactor );
    Image image = new Image( display, img.getAsBitmapForSize( display, newWidth, newHeight ), SWT.IMAGE_COPY );
    img.dispose();
    return image;
  }

  private void loadCommonImages() {


    // Icons 16x16 for buttons, toolbar items, tree items...
    //
    imageEmpty = new Image( display, 16, 16 );
    imageAdd = loadAsResource(display, "ui/images/add.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddAll = loadAsResource(display, "ui/images/add_all.svg", ConstUi.SMALL_ICON_SIZE);
    imageAddSingle = loadAsResource(display, "ui/images/add_single.svg", ConstUi.SMALL_ICON_SIZE);
    imageCalendar = loadAsResource(display, "ui/images/calendar.svg", ConstUi.SMALL_ICON_SIZE);
    imageClosePanel = loadAsResource(display, "ui/images/close-panel.svg", ConstUi.SMALL_ICON_SIZE);
    imageCollapseAll = loadAsResource(display, "ui/images/collapse-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageColor = loadAsResource(display, "ui/images/edit_option.svg", ConstUi.SMALL_ICON_SIZE);
    imageCancel = loadAsResource(display, "ui/images/cancel.svg", ConstUi.SMALL_ICON_SIZE);
    imageCopy = loadAsResource(display, "ui/images/copy.svg", ConstUi.SMALL_ICON_SIZE);
    imageCut = loadAsResource(display, "ui/images/cut.svg", ConstUi.SMALL_ICON_SIZE);
    imageDuplicate = loadAsResource(display, "ui/images/duplicate.svg", ConstUi.SMALL_ICON_SIZE);
    imagePaste = loadAsResource(display, "ui/images/paste.svg", ConstUi.SMALL_ICON_SIZE);
    imageEditOption = loadAsResource(display, "ui/images/edit_option.svg", ConstUi.SMALL_ICON_SIZE);
    imageExpandAll = loadAsResource(display, "ui/images/expand-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageLabel = loadAsResource(display, "ui/images/label.svg", ConstUi.SMALL_ICON_SIZE);
    imageFunction = loadAsResource(display, "ui/images/function.svg", ConstUi.SMALL_ICON_SIZE);
    imageForward = loadAsResource(display, "ui/images/forward.svg", ConstUi.SMALL_ICON_SIZE);
    imageHelp = loadAsResource(display, "ui/images/help.svg", ConstUi.SMALL_ICON_SIZE);
    imageHide = loadAsResource(display, "ui/images/hide.svg", ConstUi.SMALL_ICON_SIZE);
    imageHideResults = loadAsResource(display, "ui/images/hide-results.svg", ConstUi.SMALL_ICON_SIZE);
    imageHome = loadAsResource(display, "ui/images/home.svg", ConstUi.SMALL_ICON_SIZE);
    imageMaximizePanel = loadAsResource(display, "ui/images/maximize-panel.svg", ConstUi.SMALL_ICON_SIZE);
    imageMinimizePanel = loadAsResource(display, "ui/images/minimize-panel.svg", ConstUi.SMALL_ICON_SIZE);
    imageNew = loadAsResource(display, "ui/images/new.svg", ConstUi.SMALL_ICON_SIZE);
    imageNote = loadAsResource(display, "ui/images/note.svg", ConstUi.SMALL_ICON_SIZE);
    imagePrint = loadAsResource(display, "ui/images/print.svg", ConstUi.SMALL_ICON_SIZE);
    imageRefresh = loadAsResource(display, "ui/images/refresh.svg", ConstUi.SMALL_ICON_SIZE);
    imageRegEx = loadAsResource(display, "ui/images/regex.svg", ConstUi.SMALL_ICON_SIZE);
    imageRemoveAll = loadAsResource(display, "ui/images/remove_all.svg", ConstUi.SMALL_ICON_SIZE);
    imageRemoveSingle = loadAsResource(display, "ui/images/remove_single.svg", ConstUi.SMALL_ICON_SIZE);
    imageResetOption = loadAsResource(display, "ui/images/reset_option.svg", ConstUi.SMALL_ICON_SIZE);
    imageSchema = loadAsResource(display, "ui/images/user.svg", ConstUi.SMALL_ICON_SIZE);
    imageSearch = loadAsResource(display, "ui/images/search.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowAll = loadAsResource(display, "ui/images/show-all.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowErrorLines = loadAsResource(display, "ui/images/show-error-lines.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowGrid = loadAsResource(display, "ui/images/show-grid.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowHistory = loadAsResource(display, "ui/images/show-history.svg", ConstUi.SMALL_ICON_SIZE);
    imageShow = loadAsResource(display, "ui/images/show.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowLog = loadAsResource(display, "ui/images/show-log.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowPerf = loadAsResource(display, "ui/images/show-perf.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowResults = loadAsResource(display, "ui/images/show-results.svg", ConstUi.SMALL_ICON_SIZE);
    imageShowSelected = loadAsResource(display, "ui/images/show-selected.svg", ConstUi.SMALL_ICON_SIZE);
    imageSynonym = loadAsResource(display, "ui/images/view.svg", ConstUi.SMALL_ICON_SIZE);
    imageTable = loadAsResource(display, "ui/images/table.svg", ConstUi.SMALL_ICON_SIZE);
    imageUser = loadAsResource(display, "ui/images/user.svg", ConstUi.SMALL_ICON_SIZE);
    imageBack = loadAsResource(display, "ui/images/back.svg", ConstUi.SMALL_ICON_SIZE);
    imageCleanup = loadAsResource(display, "ui/images/cleanup.svg", ConstUi.SMALL_ICON_SIZE);
    imageClose = loadAsResource(display, "ui/images/close.svg", ConstUi.SMALL_ICON_SIZE);
    imageDelete = loadAsResource(display, "ui/images/delete.svg", ConstUi.SMALL_ICON_SIZE);
    imagePause = loadAsResource(display, "ui/images/pause.svg", ConstUi.SMALL_ICON_SIZE);
    imageRun = loadAsResource(display, "ui/images/run.svg", ConstUi.SMALL_ICON_SIZE);
    imageStop = loadAsResource(display, "ui/images/stop.svg", ConstUi.SMALL_ICON_SIZE);
    imageView = loadAsResource(display, "ui/images/view.svg", ConstUi.SMALL_ICON_SIZE);

    // Svg image
    //
    imageLogo = SwtSvgImageUtil.getImageAsResource( display, "ui/images/logo_icon.svg" );
    imagePipeline = SwtSvgImageUtil.getImageAsResource( display, "ui/images/pipeline.svg" );
    imageWorkflow = SwtSvgImageUtil.getImageAsResource( display, "ui/images/workflow.svg" );
    imageServer = SwtSvgImageUtil.getImageAsResource( display, "ui/images/server.svg" );
    imagePreview = SwtSvgImageUtil.getImageAsResource( display, "ui/images/preview.svg" ); 
    imageTrue = SwtSvgImageUtil.getImageAsResource( display, "ui/images/true.svg" );
    imageFalse = SwtSvgImageUtil.getImageAsResource( display, "ui/images/false.svg" );
    imageVariable = SwtSvgImageUtil.getImageAsResource( display, "ui/images/variable.svg" );    
    imageFile = SwtSvgImageUtil.getImageAsResource( display, "ui/images/file.svg" );    
    imageFolder = SwtSvgImageUtil.getImageAsResource( display, "ui/images/folder.svg" );
    imagePartitionSchema = SwtSvgImageUtil.getImageAsResource( display, "ui/images/partition_schema.svg");
    imageConnection = SwtSvgImageUtil.getImageAsResource( display, "ui/images/database.svg" );
    imageData = SwtSvgImageUtil.getImageAsResource( display, "ui/images/data.svg" );
    imageEdit = SwtSvgImageUtil.getImageAsResource( display, "ui/images/edit.svg" );
    imageMissing = SwtSvgImageUtil.getImageAsResource( display, "ui/images/missing.svg" );    
    imageDeprecated = SwtSvgImageUtil.getImageAsResource( display, "ui/images/deprecated.svg" );    
    imageLocked = SwtSvgImageUtil.getImageAsResource( display, "ui/images/lock.svg" );
    imageBol = SwtSvgImageUtil.getImageAsResource( display, "ui/images/bol.svg" );   
    imageClearText = SwtSvgImageUtil.getImageAsResource( display, "ui/images/clear-text.svg" );
    imageTransformError = SwtSvgImageUtil.getImageAsResource( display, "ui/images/show-error-lines.svg" );
    imageRedTransformError = SwtSvgImageUtil.getImageAsResource( display, "ui/images/transform-error.svg" );
    imageCopyRows = SwtSvgImageUtil.getImageAsResource( display, "ui/images/copy-rows.svg" );
    imageError = SwtSvgImageUtil.getImageAsResource( display, "ui/images/error.svg" );
    imageInfo = SwtSvgImageUtil.getImageAsResource( display, "ui/images/info.svg" );
    imageWarning = SwtSvgImageUtil.getImageAsResource( display, "ui/images/warning.svg" );    
    imageEdit = SwtSvgImageUtil.getImageAsResource( display, "ui/images/edit.svg" );
    imageInput = SwtSvgImageUtil.getImageAsResource( display, "ui/images/input.svg" );
    imageOutput = SwtSvgImageUtil.getImageAsResource( display, "ui/images/output.svg" );
    imageTarget = SwtSvgImageUtil.getImageAsResource( display, "ui/images/target.svg" );
    imageContextMenu = SwtSvgImageUtil.getImageAsResource( display, "ui/images/context_menu.svg" );
    imageParallelHop = SwtSvgImageUtil.getImageAsResource( display, "ui/images/parallel-hop.svg" );    
    imageUnconditionalHop = SwtSvgImageUtil.getImageAsResource( display, "ui/images/unconditional-hop.svg" );    
    imageBusy = SwtSvgImageUtil.getImageAsResource( display, "ui/images/busy.svg" );    
    imageInject = SwtSvgImageUtil.getImageAsResource( display, "ui/images/inject.svg" );
    imageBalance = SwtSvgImageUtil.getImageAsResource( display, "ui/images/scales.svg" );
    imageCheckpoint = SwtSvgImageUtil.getImageAsResource( display, "ui/images/checkpoint.svg" );
    imageArrow = SwtSvgImageUtil.getImageAsResource( display, "ui/images/arrow.svg" );
    imageFolderConnections = SwtSvgImageUtil.getImageAsResource( display, "ui/images/folder_connection.svg" );
    imageDisabledHop = SwtSvgImageUtil.getImageAsResource( display, "ui/images/DHOP.svg" );

    // Hop arrow
    //
    imageArrowDefault = SwtSvgImageUtil.getImageAsResource( display, "ui/images/hop-arrow-default.svg" );
    imageArrowOk = SwtSvgImageUtil.getImageAsResource( display, "ui/images/hop-arrow-ok.svg" );
    imageArrowError = SwtSvgImageUtil.getImageAsResource( display, "ui/images/hop-arrow-error.svg" );
    imageArrowDisabled = SwtSvgImageUtil.getImageAsResource( display, "ui/images/hop-arrow-disabled.svg" );
    imageArrowCandidate = SwtSvgImageUtil.getImageAsResource( display, "ui/images/hop-arrow-candidate.svg" );
  }

  public Image getImageLabel() {
  return imageLabel;}

  public Image getImageDuplicate() {
  return imageDuplicate;}

  public Image getImageCopy() {
  return imageCopy;}

  public Image getImageFunction() {
  return imageFunction;}

  public Image getImageEmpty() {
  return imageEmpty;}

  /**
   * Load all transform images from files.
   */
  private void loadWorkflowActionImages() {
    imagesActions = new Hashtable<>();

    // ACTION IMAGES TO LOAD
    //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> plugins = registry.getPlugins( ActionPluginType.class );
    for ( int i = 0; i < plugins.size(); i++ ) {
      IPlugin plugin = plugins.get( i );

      SwtUniversalImage image = null;
      String filename = plugin.getImageFile();
      try {
        ClassLoader classLoader = registry.getClassLoader( plugin );
        image = SwtSvgImageUtil.getUniversalImage( display, classLoader, filename );
      } catch ( Throwable t ) {
        log.logError( "Error occurred loading image [" + filename + "] for plugin " + plugin.getIds()[ 0 ], t );
      } finally {
        if ( image == null ) {
          log.logError( "Unable to load image [" + filename + "] for plugin " + plugin.getIds()[ 0 ] );
          image = SwtSvgImageUtil.getMissingImage( display );
        }
      }

      imagesActions.put( plugin.getIds()[ 0 ], image );
    }
  }

  /** 
   * Load all IValueMeta images from files.
   */
  private void loadValueMetaImages() {

    imagesValueMeta = new HashMap<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(ValueMetaPluginType.class);
    for (IPlugin plugin : plugins) {
        Image image = null;
        try {
          ClassLoader classLoader = registry.getClassLoader( plugin );
          image = SwtSvgImageUtil.getImage(
                  Display.getCurrent(),
                  classLoader,
                  plugin.getImageFile(),
                  ConstUi.SMALL_ICON_SIZE,
                  ConstUi.SMALL_ICON_SIZE);
        } catch (Throwable t) {
          log.logError(
              "Error occurred loading image [" + plugin.getImageFile() + "] for plugin " + plugin.getIds()[0],
              t);
        } finally {
          if (image == null) {
            log.logError(
                "Unable to load image [" + plugin.getImageFile() + "] for plugin " + plugin.getIds()[0]);
            image = this.imageLabel;
          }
        }

        imagesValueMeta.put(plugin.getIds()[ 0 ], image);
    }
  }

  /**
   * @return Returns the colorBackground.
   */
  public Color getColorBackground() {
    return colorBackground.getColor();
  }

  /**
   * @return Returns the colorBlack.
   */
  public Color getColorBlack() {
    return colorBlack.getColor();
  }

  /**
   * @return Returns the colorBlue.
   */
  public Color getColorBlue() {
    return colorBlue.getColor();
  }

  /**
   * @return Returns the colorDarkGray.
   */
  public Color getColorDarkGray() {
    return colorDarkGray.getColor();
  }

  /**
   * @return Returns the colorDemoGray.
   */
  public Color getColorDemoGray() {
    return colorDemoGray.getColor();
  }

  /**
   * @return Returns the colorDirectory.
   */
  public Color getColorDirectory() {
    return colorDirectory.getColor();
  }

  /**
   * @return Returns the colorGraph.
   */
  public Color getColorGraph() {
    return colorGraph.getColor();
  }

  /**
   * @return Returns the colorGray.
   */
  public Color getColorGray() {
    return colorGray.getColor();
  }

  /**
   * @return Returns the colorGreen.
   */
  public Color getColorGreen() {
    return colorGreen.getColor();
  }

  /**
   * @return Returns the colorLightGray.
   */
  public Color getColorLightGray() {
    return colorLightGray.getColor();
  }

  /**
   * @return Returns the colorLightBlue.
   */
  public Color getColorLightBlue() {
    return colorLightBlue.getColor();
  }

  /**
   * @return Returns the colorMagenta.
   */
  public Color getColorMagenta() {
    return colorMagenta.getColor();
  }

  /**
   * @return Returns the colorOrange.
   */
  public Color getColorOrange() {
    return colorOrange.getColor();
  }

  /**
   * @return Returns the colorSuccessGreen.
   */
  public Color getColorSuccessGreen() {
    return colorSuccessGreen.getColor();
  }

  /**
   * @return Returns the colorRed.
   */
  public Color getColorRed() {
    return colorRed.getColor();
  }

  /**
   * @return Returns the colorBlueCustomGrid.
   */
  public Color getColorBlueCustomGrid() {
    return colorBlueCustomGrid.getColor();
  }

  /**
   * @return Returns the colorTab.
   */
  public Color getColorTab() {
    return colorTab.getColor();
  }

  /**
   * @return Returns the colorWhite.
   */
  public Color getColorWhite() {
    return colorWhite.getColor();
  }

  /**
   * @return Returns the colorYellow.
   */
  public Color getColorYellow() {
    return colorYellow.getColor();
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
   * @return Returns the fontNote.
   */
  public Font getFontNote() {
    return fontNote.getFont();
  }

  /**
   * @return Returns the imageBol.
   */
  public Image getImageBol() {
    return imageBol.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageCalendar.
   */
  public Image getImageCalendar() {
    return imageCalendar;
  }

  /**
   * @return Returns the imageServer.
   */
  public Image getImageServer() {
    return imageServer.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageConnection.
   */
  public Image getImageConnection() {
    return imageConnection.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageConnection.
   */
  public Image getImageData() {
    return imageData.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageConnection() {
    return imageConnection;
  }

  public SwtUniversalImage getSwtImageData() {
    return imageData;
  }

  public Image getImageAdd() {
    return imageAdd;
  }

  /**
   * @return Returns the imageTable.
   */
  public Image getImageTable() {
    return imageTable;
  }

  /**
   * @return Returns the imageTable.
   */
  public Image getImagePreview() {
    return imagePreview.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageSchema.
   */
  public Image getImageSchema() {
    return imageSchema;
  }

  /**
   * @return Returns the imageSynonym.
   */
  public Image getImageSynonym() {
    return imageSynonym;
  }

  /**
   * @return Returns the imageView.
   */
  public Image getImageNote() {
    return imageNote;
  }

  /**
   * @return Returns the imageColor.
   */
  public Image getImageColor() {
    return imageColor;
  }

  /**
   * @return Returns the imageMissing.
   */
  public Image getImageMissing() {
    return imageMissing.getAsBitmapForSize( display, ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageMissing() {
    return imageMissing;
  }

  /**
   * @return Returns the imageHop.
   */
  public Image getImageHop() {
    return imageLogo.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageDisabledHop.
   */
  public Image getImageDisabledHop() {
    return imageDisabledHop.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imagesTransforms.
   */
  public Map<String, SwtUniversalImage> getImagesTransforms() {
    return imagesTransforms;
  }

  /**
   * @return Returns the imagesActions.
   */
  public Map<String, SwtUniversalImage> getImagesActions() {
    return imagesActions;
  }
  
  /**
   *   Return the image of the IValueMeta from plugin
   *   
   *   @return image
   */
  public Image getImage(IValueMeta valueMeta) {
    if ( valueMeta==null ) return this.imageLabel; 
    return imagesValueMeta.get(String.valueOf(valueMeta.getType()));
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
    if ( clipboard != null ) {
      clipboard.dispose();
      clipboard = null;
    }
    clipboard = new Clipboard( display );

    return clipboard;
  }

  public void toClipboard( String cliptext ) {
    if ( cliptext == null ) {
      return;
    }

    getNewClipboard();
    TextTransfer tran = TextTransfer.getInstance();
    clipboard.setContents( new String[] { cliptext }, new Transfer[] { tran } );
  }

  public String fromClipboard() {
    getNewClipboard();
    TextTransfer tran = TextTransfer.getInstance();

    return (String) clipboard.getContents( tran );
  }

  public Font getFontBold() {
    return fontBold.getFont();
  }

  private Image getZoomedImaged( SwtUniversalImage universalImage, Device device, int width, int height) {
    return universalImage.getAsBitmapForSize( device, (int)(zoomFactor*width), (int)(zoomFactor*height) );
  }

  /**
   * @return the imageVariable
   */
  public Image getImageVariable() {
    return getZoomedImaged( imageVariable, display, 10, 10 );
  }

  public Image getImagePipeline() {
    return getZoomedImaged( imagePipeline, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageUser() {
    return imageUser;
  }

  public Image getImageFolderConnections() {
    return getZoomedImaged( imagePipeline, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImagePartitionSchema() {
    return getZoomedImaged( imagePartitionSchema, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getImageWorkflow() {
    return getZoomedImaged( imageWorkflow, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getEditOptionButton() {
    return imageEditOption;
  }

  public Image getImageResetOption() {
    return imageResetOption;
  }

  /**
   * @return the imageArrow
   */
  public Image getImageArrow() {
    return getZoomedImaged( imageArrow, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageArrow() {
    return imageArrow;
  }

  /**
   * @return the imageArrow
   */
  public Image getImageFolder() {
    return getZoomedImaged( imageFolder, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return the imageFile
   */
  public Image getImageFile() {
    return getZoomedImaged( imageFile, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }  
  
  /**
   * @return the color
   */
  public Color getColorHop() {
    return colorHop.getColor();
  }

  /**
   * @return the imageLogoSmall
   */
  public Image getImageHopUi() {
    return getZoomedImaged( imageLogo, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return the colorLight
   */
  public Color getColorLight() {
    return colorLight.getColor();
  }

  /**
   * @return the colorCream
   */
  public Color getColorCream() {
    return colorCream.getColor();
  }

  /**
   * @return the default color of text in the Crystal theme
   */
  public Color getColorCrystalText() {
    return colorCrystalText.getColor();
  }

  /**
   * @return the default color the hop lines for default/unconditional
   */
  public Color getColorHopDefault() {
    return colorHopDefault.getColor();
  }

  /**
   * @return the default color the hop lines for the "OK" condition
   */
  public Color getColorHopOK() {
    return colorHopOK.getColor();
  }

  /**
   * @return the default color the deprecated condition
   */
  public Color getColorDeprecated() {
    return colorDeprecated.getColor();
  }

  public void drawGradient( Display display, GC gc, Rectangle rect, boolean vertical ) {
    if ( !vertical ) {
      gc.setForeground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.setBackground( GuiResource.getInstance().getColorHop() );
      gc.fillGradientRectangle( rect.x, rect.y, 2 * rect.width / 3, rect.height, vertical );
      gc.setForeground( GuiResource.getInstance().getColorHop() );
      gc.setBackground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.fillGradientRectangle( rect.x + 2 * rect.width / 3, rect.y, rect.width / 3 + 1, rect.height, vertical );
    } else {
      gc.setForeground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.setBackground( GuiResource.getInstance().getColorHop() );
      gc.fillGradientRectangle( rect.x, rect.y, rect.width, 2 * rect.height / 3, vertical );
      gc.setForeground( GuiResource.getInstance().getColorHop() );
      gc.setBackground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.fillGradientRectangle( rect.x, rect.y + 2 * rect.height / 3, rect.width, rect.height / 3 + 1, vertical );
    }
  }

  /**
   * Generic popup with a toggle option
   *
   * @param dialogTitle
   * @param image
   * @param message
   * @param dialogImageType
   * @param buttonLabels
   * @param defaultIndex
   * @param toggleMessage
   * @param toggleState
   * @return
   */
  public Object[] messageDialogWithToggle( Shell shell, String dialogTitle, Image image, String message,
                                           int dialogImageType, String[] buttonLabels, int defaultIndex,
                                           String toggleMessage, boolean toggleState ) {
    int imageType = 0;
    switch ( dialogImageType ) {
      case Const.WARNING:
        imageType = MessageDialog.WARNING;
        break;
      default:
        break;
    }

    MessageDialogWithToggle md =
      new MessageDialogWithToggle( shell, dialogTitle, image, message, imageType, buttonLabels, defaultIndex,
        toggleMessage, toggleState );
    int idx = md.open();
    return new Object[] { Integer.valueOf( idx ), Boolean.valueOf( md.getToggleState() ) };
  }

  public static Point calculateControlPosition( Control control ) {
    // Calculate the exact location...
    //
    Rectangle r = control.getBounds();
    Point p = control.getParent().toDisplay( r.x, r.y );

    return p;

    /*
     * Point location = control.getLocation();
     *
     * Composite parent = control.getParent(); while (parent!=null) {
     *
     * Composite newParent = parent.getParent(); if (newParent!=null) { location.x+=parent.getLocation().x;
     * location.y+=parent.getLocation().y; } else { if (parent instanceof Shell) { // Top level shell. Shell shell =
     * (Shell)parent; Rectangle bounds = shell.getBounds(); Rectangle clientArea = shell.getClientArea(); location.x +=
     * bounds.width-clientArea.width; location.y += bounds.height-clientArea.height; } } parent = newParent; }
     *
     * return location;
     */
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

  /**
   * @return the imageShowLog
   */
  public Image getImageShowLog() {
    return imageShowLog;
  }

  /**
   * @return the imageShowGrid
   */
  public Image getImageShowGrid() {
    return imageShowGrid;
  }

  /**
   * @return the imageShowHistory
   */
  public Image getImageShowHistory() {
    return imageShowHistory;
  }

  /**
   * @return the imageShowPerf
   */
  public Image getImageShowPerf() {
    return imageShowPerf;
  }

  /**
   * @return the "hide" image
   */
  public Image getImageHide() {
    return imageHide;
  }

  /**
   * @return the "show" image
   */
  public Image getImageShow() {
    return imageShow;
  }

  /**
   * @return the "show selected" image
   */
  public Image getImageShowSelected() {
    return imageShowSelected;
  }

  /**
   * @return the "show all" image
   */
  public Image getImageShowAll() {
    return imageShowAll;
  }

  /**
   * @return the close panel image
   */
  public Image getImageClosePanel() {
    return imageClosePanel;
  }

  /**
   * @return the maximize panel image
   */
  public Image getImageMaximizePanel() {
    return imageMaximizePanel;
  }

  /**
   * @return the minimize panel image
   */
  public Image getImageMinimizePanel() {
    return imageMinimizePanel;
  }

  /**
   * @return the show error lines image
   */
  public Image getImageShowErrorLines() {
    return imageShowErrorLines;
  }

  public Image getImageShowResults() {
    return imageShowResults;
  }

  public Image getImageHideResults() {
    return imageHideResults;
  }

  public Image getImageClearText() {
    return getZoomedImaged( imageClearText, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageExpandAll() {
    return imageExpandAll;
  }

  public Image getImageRegex() {
    return imageRegEx;
  }

  public Image getImageCollapseAll() {
    return imageCollapseAll;
  }

  public Image getImageTransformError() {
    return getZoomedImaged( imageTransformError, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageTransformError() {
    return imageTransformError;
  }

  public Image getImageRedTransformError() {
    return getZoomedImaged( imageRedTransformError, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageRedTransformError() {
    return imageRedTransformError;
  }

  public Image getImageCopyHop() {
    return getZoomedImaged( imageCopyRows, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageCopyRows() {
    return imageCopyRows;
  }

  public Image getImageErrorHop() {
    return getZoomedImaged( imageError, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageError() {
    return imageError;
  }

  public Image getImageInfo() {
    return getZoomedImaged( imageInfo, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInfo() {
    return imageInfo;
  }

  public Image getImageWarning() {
    return getZoomedImaged( imageWarning, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageWarning() {
    return imageWarning;
  }

  public SwtUniversalImage getSwtImageDeprecated() {
    return imageDeprecated;
  }
  
  public Image getImageDeprecated() {
    return getZoomedImaged( imageDeprecated, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageNew() {
    return imageNew;
  }

  public Image getImageEdit() {
    return getZoomedImaged( imageEdit, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageEdit() {
    return imageEdit;
  }

  public Image getImageDelete() {
    return imageDelete;
  }

  public Image getImageCancel() {
    return imageCancel;
  }

  public Image getImageCut() {
    return imageCut;
  }

  public Image getImageInput() {
    return getZoomedImaged( imageInput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInput() {
    return imageInput;
  }

  public Image getImageOutput() {
    return getZoomedImaged( imageOutput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageOutput() {
    return imageOutput;
  }

  public Image getImageTarget() {
    return getZoomedImaged( imageTarget, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageTarget() {
    return imageTarget;
  }

  public Image getImageLocked() {
    return getZoomedImaged( imageLocked, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageLocked() {
    return imageLocked;
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of this, it's important
   * to never dispose of the image you get from here. (easy!) The images are automatically disposed when the application
   * ends.
   *
   * @param location the location of the image resource to load
   * @return the loaded image
   */
  public Image getImage( String location ) {
    return getImage( location, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
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
      image = new Image(display, svg.getAsBitmapForSize(display, realWidth, realHeight), SWT.IMAGE_COPY);
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
    // Build image key for a specific size
    StringBuilder builder = new StringBuilder(location);
    builder.append('|');
    builder.append(width);
    builder.append('|');
    builder.append(height);
    String key = builder.toString();
    
    Image image = imageMap.get(key);
    if (image == null) {
      SwtUniversalImage svg = SwtSvgImageUtil.getUniversalImage(display, classLoader, location);
      image = new Image(display, getZoomedImaged(svg, display, width, height), SWT.IMAGE_COPY);
      svg.dispose();
      imageMap.put(location, image);
    }
    return image;
  }

  public Color getColor( int red, int green, int blue ) {
    RGB rgb = new RGB( red, green, blue );
    Color color = colorMap.get( rgb );
    if ( color == null ) {
      color = new Color( display, rgb );
      colorMap.put( rgb, color );
    }
    return color;
  }

  /**
   * @return The image map used to cache images loaded from certain location using getImage(String location);
   */
  public Map<String, Image> getImageMap() {
    return imageMap;
  }

  /**
   * @return the imageTrue
   */
  public Image getImageTrue() {
    return getZoomedImaged( imageTrue, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageTrue() {
    return imageTrue;
  }

  /**
   * @return the imageFalse
   */
  public Image getImageFalse() {
    return getZoomedImaged( imageFalse, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageFalse() {
    return imageFalse;
  }

  /**
   * @return the imageContextMenu
   */
  public Image getImageContextMenu() {
    return getZoomedImaged( imageContextMenu, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageContextMenu() {
    return imageContextMenu;
  }

  public Image getImageParallelHop() {
    return getZoomedImaged( imageParallelHop, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageParallelHop() {
    return imageParallelHop;
  }

  public Image getImageUnconditionalHop() {
    return getZoomedImaged( imageUnconditionalHop, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageUnconditionalHop() {
    return imageUnconditionalHop;
  }

  public Image getImageBusy() {
    return getZoomedImaged( imageBusy, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageBusy() {
    return imageBusy;
  }

  public Image getImageInject() {
    return getZoomedImaged( imageInject, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInject() {
    return imageInject;
  }

  public Image getImageBalance() {
    return getZoomedImaged( imageBalance, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageBalance() {
    return imageBalance;
  }

  public Image getImageCheckpoint() {
    return getZoomedImaged( imageCheckpoint, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageCheckpoint() {
    return imageCheckpoint;
  }

  public Image getImageHelpWeb() {
    return imageHelp;
  }

  public Image getImageAddAll() {
    return imageAddAll;
  }

  public Image getImageAddSingle() {
    return imageAddSingle;
  }

  public Image getImageRemoveAll() {
    return imageRemoveAll;
  }

  public Image getImageRemoveSingle() {
    return imageRemoveSingle;
  }

  public Image getImageBack() {
    return imageBack;
  }

  public Image getImageForward() {
    return imageForward;
  }

  public Image getImageRefresh() {
    return imageRefresh;
  }

  public Image getImageHome() {
    return imageHome;
  }

  public Image getImagePrint() {
    return imagePrint;
  }

  public SwtUniversalImage getSwtImageArrowDefault() {
    return imageArrowDefault;
  }

  public SwtUniversalImage getSwtImageArrowOk() {
    return imageArrowOk;
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

  /**
   * Gets imageToolbarCleanup
   *
   * @return value of imageToolbarCleanup
   */
  public Image getImageCleanup() {
    return imageCleanup;
  }

  /**
   * Gets imageToolbarClose
   *
   * @return value of imageToolbarClose
   */
  public Image getImageClose() {
    return imageClose;
  }

  /**
   * Gets imageToolbarPause
   *
   * @return value of imageToolbarPause
   */
  public Image getImagePause() {
    return imagePause;
  }

  /**
   * Gets image run 16x16
   * 
   * @see {@link GuiResource#getImageStop}, {@link GuiResource#getImagePause}
   * @return image
   */
  public Image getImageRun() {
    return imageRun;
  }

  /**
   * Gets image stop 16x16
   * 
   * @see {@link GuiResource#getImageRun}, {@link GuiResource#getImagePause}
   * @return image
   */
  public Image getImageStop() {
    return imageStop;
  }

  /**
   * Gets imageToolbarView 
   *
   * @return value of imageToolbarView
   */
  public Image getImageView() {
    return imageView;
  }

  /**
   * Gets image search 16x16
   *
   * @return image
   */
  public Image getImageSearch() {
    return imageSearch;
  }

}
