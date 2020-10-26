//CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

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
import org.apache.hop.laf.BasePropertyHandler;
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

  private Map<String, Image> imagesTransformsSmall = new Hashtable<>();

  private Map<String, SwtUniversalImage> imagesActions;

  private Map<String, Image> imagesActionsSmall;

  private SwtUniversalImage imageHop;

  private SwtUniversalImage imageDisabledHop;

  private SwtUniversalImage imageConnection;
  private SwtUniversalImage imageData;

  private SwtUniversalImage imageConnectionTree;

  private Image imageAdd;

  private Image imageTable;

  private SwtUniversalImage imagePreview;

  private Image imageSchema;

  private Image imageSynonym;

  private Image imageView;

  private SwtUniversalImage imageLogoSmall;

  private SwtUniversalImage imageBol;

  private Image imageCalendar;

  private SwtUniversalImage imageServer;

  private SwtUniversalImage imageArrow;

  private SwtUniversalImage imageFolder;

  private Image imageWizard;

  private SwtUniversalImage imageStart;

  private SwtUniversalImage imageDummy;

  private SwtUniversalImage imageMissing;

  private SwtUniversalImage imageHopUi;

  private SwtUniversalImage imageVariable;

  private SwtUniversalImage imagePipelineGraph;

  private SwtUniversalImage imagePartitionSchema;

  private SwtUniversalImage imageWorkflowGraph;

  private SwtUniversalImage imagePipelineTree;

  private SwtUniversalImage imageWorkflowTree;

  private SwtUniversalImage defaultArrow;
  private SwtUniversalImage okArrow;
  private SwtUniversalImage errorArrow;
  private SwtUniversalImage disabledArrow;
  private SwtUniversalImage candidateArrow;

  private Image imageUser;

  private SwtUniversalImage imageFolderConnections;

  private Image imageEditOptionButton;

  private Image imageEditSmall;

  private Image imageExploreSolutionSmall;

  private Image imageColor;

  private Image imageNoteSmall;

  private Image imageResetOptionButton;

  private Image imageShowLog;

  private Image imageShowGrid;

  private Image imageShowHistory;

  private Image imageShowPerf;

  private Image imageShowInactive;

  private Image imageHideInactive;

  private Image imageShowSelected;

  private Image imageShowAll;

  private Image imageClosePanel;

  private Image imageMaximizePanel;

  private Image imageMinimizePanel;

  private Image imageShowErrorLines;

  private Image imageShowResults;

  private Image imageHideResults;

  private SwtUniversalImage imageExpandAll;

  private SwtUniversalImage imageClearText;

  private SwtUniversalImage imageClearTextDisabled;

  private Image imageSearchSmall;

  private Image imageRegExSmall;

  private SwtUniversalImage imageCollapseAll;

  private SwtUniversalImage imageTransformError;

  private SwtUniversalImage imageRedTransformError;

  private SwtUniversalImage imageCopyHop;

  private SwtUniversalImage imageErrorHop;

  private SwtUniversalImage imageInfoHop;

  private SwtUniversalImage imageWarning;

  private Image imageDeprecated;

  private Image imageNew;

  private SwtUniversalImage imageEdit;

  private Image imageDelete;

  private Image imagePauseLog;

  private Image imageContinueLog;

  private SwtUniversalImage imageHopInput;

  private SwtUniversalImage imageHopOutput;

  private SwtUniversalImage imageHopTarget;

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

  private Image imageHelpWeb;

  /**
   * Same result as <code>new Image(display, 16, 16)</code>.
   */
  private Image imageEmpty16x16;

  private Map<String, Image> imageMap;

  private Map<RGB, Color> colorMap;

  private Image imageAddAll;

  private Image imageAddSingle;

  private Image imageRemoveAll;

  private Image imageRemoveSingle;

  private SwtUniversalImage imageBackEnabled;

  private SwtUniversalImage imageBackDisabled;

  private SwtUniversalImage imageForwardEnabled;

  private SwtUniversalImage imageForwardDisabled;

  private SwtUniversalImage imageRefreshEnabled;

  private SwtUniversalImage imageRefreshDisabled;

  private SwtUniversalImage imageHomeEnabled;

  private SwtUniversalImage imageHomeDisabled;

  private SwtUniversalImage imagePrintEnabled;

  private SwtUniversalImage imagePrintDisabled;

  private Image imageToolbarBack;
  private Image imageToolbarCleanup;
  private Image imageToolbarClose;
  private Image imageToolbarPipeline;
  private Image imageToolbarWorkflow;
  private Image imageToolbarPause;
  private Image imageToolbarRun;
  private Image imageToolbarRunOption;
  private Image imageToolbarStop;
  private Image imageToolbarView;
  private Image imageToolbarViewAsXml;

  private Image imageToolbarDataOrchestration;
  private Image imageToolbarDataOrchestrationInactive;

  private Image imageToolbarSearch;
  private Image imageToolbarSearchInactive;

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
    imageMap = new HashMap<String, Image>();
    colorMap = new HashMap<RGB, Color>();

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
      imageHop.dispose();
      imageDisabledHop.dispose();
      imageConnection.dispose();
      imageData.dispose();
      imageConnectionTree.dispose();
      imageAdd.dispose();
      imageTable.dispose();
      imagePreview.dispose();
      imageSchema.dispose();
      imageSynonym.dispose();
      imageView.dispose();
      imageLogoSmall.dispose();
      imageBol.dispose();
      imageCalendar.dispose();
      imageServer.dispose();
      imageArrow.dispose();
      imageFolder.dispose();
      imageWizard.dispose();
      imageStart.dispose();
      imageDummy.dispose();
      imageMissing.dispose();
      imageHopUi.dispose();
      imageVariable.dispose();
      imagePipelineGraph.dispose();
      imagePartitionSchema.dispose();
      imageWorkflowGraph.dispose();
      imagePipelineTree.dispose();
      imageWorkflowTree.dispose();
      imageUser.dispose();
      imageFolderConnections.dispose();
      imageShowResults.dispose();
      imageHideResults.dispose();
      imageCollapseAll.dispose();
      imageTransformError.dispose();
      imageRedTransformError.dispose();
      imageCopyHop.dispose();
      imageErrorHop.dispose();
      imageInfoHop.dispose();
      imageWarning.dispose();
      imageClearText.dispose();
      imageDeprecated.dispose();
      imageClearTextDisabled.dispose();
      imageExpandAll.dispose();
      imageSearchSmall.dispose();
      imageRegExSmall.dispose();
      imageNew.dispose();
      imageEdit.dispose();
      imageDelete.dispose();
      imagePauseLog.dispose();
      imageContinueLog.dispose();
      imageLocked.dispose();
      imageHopInput.dispose();
      imageHopOutput.dispose();
      imageHopTarget.dispose();
      imageTrue.dispose();
      imageFalse.dispose();
      imageContextMenu.dispose();
      imageParallelHop.dispose();
      imageUnconditionalHop.dispose();
      imageBusy.dispose();
      imageEmpty16x16.dispose();
      imageInject.dispose();
      imageBalance.dispose();
      imageCheckpoint.dispose();
      imageHelpWeb.dispose();
      imageAddAll.dispose();
      imageAddSingle.dispose();
      imageRemoveAll.dispose();
      imageRemoveSingle.dispose();
      imageBackEnabled.dispose();
      imageBackDisabled.dispose();
      imageForwardEnabled.dispose();
      imageForwardDisabled.dispose();
      imageRefreshEnabled.dispose();
      imageRefreshDisabled.dispose();
      imageHomeEnabled.dispose();
      imageHomeDisabled.dispose();
      imagePrintEnabled.dispose();
      imagePrintDisabled.dispose();

      defaultArrow.dispose();
      okArrow.dispose();
      errorArrow.dispose();
      disabledArrow.dispose();
      candidateArrow.dispose();

      disposeImage( imageNoteSmall );
      disposeImage( imageColor );
      disposeImage( imageEditOptionButton );
      disposeImage( imageResetOptionButton );

      disposeImage( imageEditSmall );
      disposeImage( imageExploreSolutionSmall );

      disposeImage( imageShowLog );
      disposeImage( imageShowGrid );
      disposeImage( imageShowHistory );
      disposeImage( imageShowPerf );

      disposeImage( imageShowInactive );
      disposeImage( imageHideInactive );

      disposeImage( imageShowSelected );
      disposeImage( imageShowAll );

      disposeImage( imageClosePanel );
      disposeImage( imageMaximizePanel );
      disposeImage( imageMinimizePanel );

      disposeImage( imageShowErrorLines );

      // Toolbar images
      //
      // Toolbar icons
      //
      imageToolbarBack.dispose();
      imageToolbarCleanup.dispose();
      imageToolbarClose.dispose();
      imageToolbarWorkflow.dispose();
      imageToolbarPipeline.dispose();
      imageToolbarPause.dispose();
      imageToolbarRun.dispose();
      imageToolbarRunOption.dispose();
      imageToolbarStop.dispose();
      imageToolbarDataOrchestration.dispose();
      imageToolbarDataOrchestrationInactive.dispose();
      imageToolbarSearch.dispose();
      imageToolbarSearchInactive.dispose();
      imageToolbarView.dispose();
      imageToolbarViewAsXml.dispose();

      // big images
      disposeUniversalImages( imagesTransforms.values() );

      // Small images
      disposeImages( imagesTransformsSmall.values() );

      // Dispose of the images in the map
      disposeImages( imageMap.values() );
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
    // imagesTransforms.clear();
    // imagesTransformsSmall.clear();

    //
    // TRANSFORM IMAGES TO LOAD
    //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> transforms = registry.getPlugins( TransformPluginType.class );
    for ( IPlugin transform : transforms ) {
      if ( imagesTransforms.get( transform.getIds()[ 0 ] ) != null ) {
        continue;
      }

      SwtUniversalImage image = null;
      Image smallImage = null;

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

      // Calculate the smaller version of the image @ 16x16...
      // Perhaps we should make this configurable?
      //
      smallImage = image.getAsBitmapForSize( display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );

      imagesTransforms.put( transform.getIds()[ 0 ], image );
      imagesTransformsSmall.put( transform.getIds()[ 0 ], smallImage );
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
    // "ui/images/HOP.png"
    imageHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HOP_image" ) );

    imageDisabledHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Disabled_HOP_image" ) );

    // "ui/images/CNC.svg"
    imageConnection = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CNC_image" ) );

    // "ui/images/CNC.svg"
    imageData = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Data_image" ) );

    // "ui/images/CNC_tree"
    imageConnectionTree = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CNC_tree_image" ) );

    // "ui/images/Add.svg"
    imageAdd = loadAsResource( display, BasePropertyHandler.getProperty( "Add_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/table.svg"
    imageTable = loadAsResource( display, BasePropertyHandler.getProperty( "Table_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/preview.svg"
    imagePreview = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Preview_image" ) );

    // "ui/images/schema.svg"
    imageSchema = loadAsResource( display, BasePropertyHandler.getProperty( "Schema_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/synonym.svg"
    imageSynonym = loadAsResource( display, BasePropertyHandler.getProperty( "Synonym_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/view.svg"
    imageView = loadAsResource( display, BasePropertyHandler.getProperty( "View_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/color.svg.svg"
    imageColor = loadAsResource( display, BasePropertyHandler.getProperty( "Color_image" ), 12 );

    // "ui/images/noteSmall.svg"
    imageNoteSmall = loadAsResource( display, BasePropertyHandler.getProperty( "Note_image" ), 12 );

    // , "ui/images/server.svg"
    imageServer = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Server_image" ) );

    // "ui/images/BOL.svg"
    imageBol = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BOL_image" ) );

    // "ui/images/Calendar.svg"
    imageCalendar = loadAsResource( display, BasePropertyHandler.getProperty( "Calendar_image" ), ConstUi.SMALL_ICON_SIZE ); // ,

    // "ui/images/STR.svg"
    imageStart = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "STR_image" ) );

    // "ui/images/DUM.svg"
    imageDummy = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "DUM_image" ) );

    //ui/images/missing_entry.svg
    imageMissing = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "MIS_image" ) );

    // "ui/images/hop-logo.svg"
    imageHopUi = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "hop_icon" ) );

    // "ui/images/variable.svg"
    imageVariable = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Variable_image" ) );

    // "ui/images/edit_option.svg"
    imageEditOptionButton = loadAsResource( display, BasePropertyHandler.getProperty( "EditOption_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/reset_option.svg"
    imageResetOptionButton = loadAsResource( display, BasePropertyHandler.getProperty( "ResetOption_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/Edit.svg"
    imageEditSmall = loadAsResource( display, BasePropertyHandler.getProperty( "EditSmall_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/exploreSolution.svg"
    imageExploreSolutionSmall = loadAsResource( display, BasePropertyHandler.getProperty( "ExploreSolutionSmall_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-log.svg"
    imageShowLog = loadAsResource( display, BasePropertyHandler.getProperty( "ShowLog_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-grid.svg"
    imageShowGrid = loadAsResource( display, BasePropertyHandler.getProperty( "ShowGrid_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-history.svg"
    imageShowHistory = loadAsResource( display, BasePropertyHandler.getProperty( "ShowHistory_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-perf.svg"
    imageShowPerf = loadAsResource( display, BasePropertyHandler.getProperty( "ShowPerf_image" ), ConstUi.SMALL_ICON_SIZE );

    // ui/images/show-inactive-selected.svg
    imageShowInactive = loadAsResource( display, BasePropertyHandler.getProperty( "ShowInactive_image" ), ConstUi.SMALL_ICON_SIZE );

    // ui/images/show-inactive-selected.svg
    imageHideInactive = loadAsResource( display, BasePropertyHandler.getProperty( "HideInactive_image" ), ConstUi.SMALL_ICON_SIZE );

    // ui/images/show-selected.svg
    imageShowSelected = loadAsResource( display, BasePropertyHandler.getProperty( "ShowSelected_image" ), ConstUi.SMALL_ICON_SIZE );

    // ui/images/show-all.svg
    imageShowAll = loadAsResource( display, BasePropertyHandler.getProperty( "ShowAll_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-perf.svg"
    imageClosePanel = loadAsResource( display, BasePropertyHandler.getProperty( "ClosePanel_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-perf.svg"
    imageMaximizePanel = loadAsResource( display, BasePropertyHandler.getProperty( "MaximizePanel_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-perf.svg"
    imageMinimizePanel = loadAsResource( display, BasePropertyHandler.getProperty( "MinimizePanel_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-perf.svg"
    imageShowErrorLines = loadAsResource( display, BasePropertyHandler.getProperty( "ShowErrorLines_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/show-results.svg
    imageShowResults = loadAsResource( display, BasePropertyHandler.getProperty( "ShowResults_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/hide-results.svg
    imageHideResults = loadAsResource( display, BasePropertyHandler.getProperty( "HideResults_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/ClearText.svg;
    imageClearText = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ClearText_image" ) );

    // "ui/images/ClearTextDisabled.svg;
    imageClearTextDisabled = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ClearTextDisabled_image" ) );

    // "ui/images/ExpandAll.svg;
    imageExpandAll = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ExpandAll_image" ) );

    // "ui/images/CollapseAll.svg;
    imageCollapseAll = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CollapseAll_image" ) );

    // "ui/images/show-error-lines.svg;
    imageTransformError = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "TransformErrorLines_image" ) );

    // "ui/images/transform-error.svg;
    imageRedTransformError = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "TransformErrorLinesRed_image" ) );

    // "ui/images/copy-hop.svg;
    imageCopyHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CopyHop_image" ) );

    // "ui/images/error-hop.svg;
    imageErrorHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ErrorHop_image" ) );

    // "ui/images/info-hop.svg;
    imageInfoHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "InfoHop_image" ) );

    // "ui/images/warning.svg;
    imageWarning = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Warning_image" ) );

    // "ui/images/deprecated.svg
    imageDeprecated = loadAsResource( display, BasePropertyHandler.getProperty( "Deprecated_image" ), ConstUi.LARGE_ICON_SIZE );


    // "ui/images/generic-new.svg;
    imageNew = loadAsResource( display, BasePropertyHandler.getProperty( "Add_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/generic-edit.svg;
    imageEdit = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "EditSmall_image" ) );

    // "ui/images/generic-delete.svg;
    imageDelete = loadAsResource( display, BasePropertyHandler.getProperty( "DeleteOriginal_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/pause-log.svg;
    imagePauseLog = loadAsResource( display, BasePropertyHandler.getProperty( "PauseLog_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/continue-log.svg;
    imageContinueLog = loadAsResource( display, BasePropertyHandler.getProperty( "ContinueLog_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/hop-input.svg;
    imageHopInput = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopInput_image" ) );

    // "ui/images/hop-output.svg;
    imageHopOutput = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopOutput_image" ) );

    // "ui/images/hop-target.svg;
    imageHopTarget = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopTarget_image" ) );

    // "ui/images/locked.svg;
    imageLocked = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Locked_image" ) );

    // "ui/images/true.svg;
    imageTrue = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "True_image" ) );

    // "ui/images/false.svg;
    imageFalse = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "False_image" ) );

    // "ui/images/context_menu.svg;
    imageContextMenu = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ContextMenu_image" ) );

    // "ui/images/parallel-hop.svg
    imageParallelHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ParallelHop_image" ) );

    // "ui/images/unconditional-hop.svg
    imageUnconditionalHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "UnconditionalHop_image" ) );

    // "ui/images/busy.svg
    imageBusy = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Busy_image" ) );

    // "ui/images/inject.svg
    imageInject = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Inject_image" ) );

    // "ui/images/scales.svg
    imageBalance = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "LoadBalance_image" ) );

    // "ui/images/scales.svg
    imageCheckpoint = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CheckeredFlag_image" ) );

    // "ui/images/help_web.svg
    imageHelpWeb = loadAsResource( display, BasePropertyHandler.getProperty( "HelpWeb_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/add_all.svg
    imageAddAll = loadAsResource( display, BasePropertyHandler.getProperty( "AddAll_image" ), 12 );

    // "ui/images/add_single.svg
    imageAddSingle = loadAsResource( display, BasePropertyHandler.getProperty( "AddSingle_image" ), 12 );

    // "ui/images/remove_all.svg
    imageRemoveAll = loadAsResource( display, BasePropertyHandler.getProperty( "RemoveAll_image" ), 12 );

    // "ui/images/remove_single.svg
    imageRemoveSingle = loadAsResource( display, BasePropertyHandler.getProperty( "RemoveSingle_image" ), 12 );

    // ui/images/back-enabled.svg
    imageBackEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BackEnabled" ) );

    // ui/images/back-disabled.svg
    imageBackDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BackDisabled" ) );

    // ui/images/forward-enabled.svg
    imageForwardEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ForwardEnabled" ) );

    // ui/images/forward-disabled.svg
    imageForwardDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ForwardDisabled" ) );

    // ui/images/refresh-enabled.svg
    imageRefreshEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "RefreshEnabled" ) );

    // ui/images/refresh-disabled.svg
    imageRefreshDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "RefreshDisabled" ) );

    // ui/images/home-enabled.svg
    imageHomeEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "HomeEnabled" ) );

    // ui/images/home-disabled.svg
    imageHomeDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "HomeDisabled" ) );

    // ui/images/print-enabled.svg
    imagePrintEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PrintEnabled" ) );

    // ui/images/print-disabled.svg
    imagePrintDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PrintDisabled" ) );

    imageEmpty16x16 = new Image( display, 16, 16 );

    imagePipelineGraph = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PipelineGraph_image" ) );

    imagePartitionSchema = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Image_Partition_Schema" ) );

    imageWorkflowGraph = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "WorkflowGraph_image" ) );

    imagePipelineTree = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Pipeline_tree_image" ) );
    imageWorkflowTree = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Workflow_tree_image" ) );

    // "ui/images/hop-logo.svg"
    imageLogoSmall = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Logo_sml_image" ) );

    // "ui/images/arrow.svg"
    imageArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ArrowIcon_image" ) );

    // "ui/images/folder.svg"
    imageFolder = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Folder_image" ) );

    // Makes transparent images "on the fly"
    //

    // "ui/images/wizard.svg"
    imageWizard = loadAsResource( display, BasePropertyHandler.getProperty( "spoon_icon" ), 0 );

    // , "ui/images/user.svg"
    imageUser = loadAsResource( display, BasePropertyHandler.getProperty( "User_image" ), ConstUi.SMALL_ICON_SIZE );

    // "ui/images/folder_connection.svg"
    imageFolderConnections = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "FolderConnections_image" ) );

    imageRegExSmall = loadAsResource( display, BasePropertyHandler.getProperty( "RegExSmall_image" ), ConstUi.SMALL_ICON_SIZE );
    imageSearchSmall = loadAsResource( display, BasePropertyHandler.getProperty( "SearchSmall_image" ), ConstUi.SMALL_ICON_SIZE );

    defaultArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "defaultArrow_image" ) );
    okArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "okArrow_image" ) );
    errorArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "errorArrow_image" ) );
    disabledArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "disabledArrow_image" ) );
    candidateArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "candidateArrow_image" ) );


    // Toolbar icons
    //
    imageToolbarBack = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarBack_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarCleanup = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarCleanup_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarClose = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarClose_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarWorkflow = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarWorkflow_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarPause = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarPause_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarRun = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarRun_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarRunOption = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarRunOption_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarStop = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarStop_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarDataOrchestration = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarDataOrchestration_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarDataOrchestrationInactive = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarDataOrchestration_Inactive_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarPipeline = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarPipeline_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarSearch = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarSearch_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarSearchInactive = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarSearchInactive_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarView = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarView_image" ), ConstUi.SMALL_ICON_SIZE );
    imageToolbarViewAsXml = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarViewAsXml_image" ), ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * Load all transform images from files.
   */
  private void loadWorkflowActionImages() {
    imagesActions = new Hashtable<>();
    imagesActionsSmall = new Hashtable<>();

    // //
    // // ACTION IMAGES TO LOAD
    // //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> plugins = registry.getPlugins( ActionPluginType.class );
    for ( int i = 0; i < plugins.size(); i++ ) {
      IPlugin plugin = plugins.get( i );

      if ( "SPECIAL".equals( plugin.getIds()[ 0 ] ) ) {
        continue;
      }

      SwtUniversalImage image = null;
      Image small_image = null;

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
      // Calculate the smaller version of the image @ 16x16...
      // Perhaps we should make this configurable?
      //
      if ( image != null ) {
        small_image = image.getAsBitmapForSize( display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
      }

      imagesActions.put( plugin.getIds()[ 0 ], image );
      imagesActionsSmall.put( plugin.getIds()[ 0 ], small_image );
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

  public Image getImageConnectionTree() {
    return imageConnectionTree.getAsBitmapForSize( display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
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
  public Image getImageView() {
    return imageView;
  }

  /**
   * @return Returns the imageView.
   */
  public Image getImageNoteSmall() {
    return imageNoteSmall;
  }

  /**
   * @return Returns the imageColor.
   */
  public Image getImageColor() {
    return imageColor;
  }

  /**
   * @return Returns the imageDummy.
   */
  public Image getImageDummy() {
    return imageDummy.getAsBitmapForSize( display, ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageDummy() {
    return imageDummy;
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
    return imageHop.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageDisabledHop.
   */
  public Image getImageDisabledHop() {
    return imageDisabledHop.getAsBitmapForSize( display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageHop.
   */
  public Image getImageHopTree() {
    return imageHop.getAsBitmapForSize( display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageDisabledHop.
   */
  public Image getImageDisabledHopTree() {
    return imageDisabledHop.getAsBitmapForSize( display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageHopGui.
   */
  public Image getImageHopUi() {
    return imageHopUi.getAsBitmapForSize( display, ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  /**
   * @return Returns the imagesTransforms.
   */
  public Map<String, SwtUniversalImage> getImagesTransforms() {
    return imagesTransforms;
  }

  /**
   * @return Returns the imagesTransformsSmall.
   */
  public Map<String, Image> getImagesTransformsSmall() {
    return imagesTransformsSmall;
  }

  /**
   * @return Returns the imageStart.
   */
  public Image getImageStart() {
    return imageStart.getAsBitmapForSize( display, ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageStart() {
    return imageStart;
  }

  /**
   * @return Returns the imagesActions.
   */
  public Map<String, SwtUniversalImage> getImagesActions() {
    return imagesActions;
  }

  /**
   * @param imagesActions The imagesActions to set.
   */
  public void setImagesActions( Hashtable<String, SwtUniversalImage> imagesActions ) {
    this.imagesActions = imagesActions;
  }

  /**
   * @return Returns the imagesActionsSmall.
   */
  public Map<String, Image> getImagesActionsSmall() {
    return imagesActionsSmall;
  }

  /**
   * @param imagesActionsSmall The imagesActionsSmall to set.
   */
  public void setImagesActionsSmall( Hashtable<String, Image> imagesActionsSmall ) {
    this.imagesActionsSmall = imagesActionsSmall;
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

  public Image getImagePipelineGraph() {
    return getZoomedImaged( imagePipelineGraph, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImagePipelineTree() {
    return getZoomedImaged( imagePipelineTree, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getImageUser() {
    return imageUser;
  }

  public Image getImageFolderConnections() {
    return getZoomedImaged( imagePipelineGraph, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageFolderConnectionsMedium() {
    return getZoomedImaged( imagePipelineGraph, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getImagePartitionSchema() {
    return getZoomedImaged( imagePartitionSchema, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getImageWorkflowGraph() {
    return getZoomedImaged( imageWorkflowGraph, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageWorkflowTree() {
    return getZoomedImaged( imageWorkflowTree, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getEditOptionButton() {
    return imageEditOptionButton;
  }

  public Image getResetOptionButton() {
    return imageResetOptionButton;
  }

  public Image getImageEditSmall() {
    return imageEditSmall;
  }

  public Image getImageExploreSolutionSmall() {
    return imageExploreSolutionSmall;
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
    return getZoomedImaged( imageFolder, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageDummySmall
   */
  public Image getImageDummySmall() {
    return getZoomedImaged( imageDummy, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return the imageStartSmall
   */
  public Image getImageStartSmall() {
    return getZoomedImaged( imageStart, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  /**
   * @return the imageDummyMedium
   */
  public Image getImageDummyMedium() {
    return getZoomedImaged( imageDummy, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageStartSmall
   */
  public Image getImageStartMedium() {
    return getZoomedImaged( imageStart, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageWizard
   */
  public Image getImageWizard() {
    return imageWizard;
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
  public Image getImageLogoSmall() {
    return getZoomedImaged( imageLogoSmall, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
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
   * @return the "hide inactive" image
   */
  public Image getImageHideInactive() {
    return imageHideInactive;
  }

  /**
   * @return the "show inactive" image
   */
  public Image getImageShowInactive() {
    return imageShowInactive;
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

  public Image getImageClearTextDisabled() {
    return getZoomedImaged( imageClearTextDisabled, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageExpandAll() {
    return getZoomedImaged( imageExpandAll, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageExpandAllMedium() {
    return getZoomedImaged( imageExpandAll, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
  }

  public Image getImageSearchSmall() {
    return imageSearchSmall;
  }

  public Image getImageRegexSmall() {
    return imageRegExSmall;
  }

  public Image getImageCollapseAll() {
    return getZoomedImaged( imageCollapseAll, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageCollapseAllMedium() {
    return getZoomedImaged( imageCollapseAll, display, ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE );
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
    return getZoomedImaged( imageCopyHop, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageCopyHop() {
    return imageCopyHop;
  }

  public Image getImageErrorHop() {
    return getZoomedImaged( imageErrorHop, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageErrorHop() {
    return imageErrorHop;
  }

  public Image getImageInfoHop() {
    return getZoomedImaged( imageInfoHop, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInfoHop() {
    return imageInfoHop;
  }

  public Image getImageWarning() {
    return getZoomedImaged( imageWarning, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public Image getImageWarning32() {
    return getZoomedImaged( imageWarning, display, ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageWarning() {
    return imageWarning;
  }

  public Image getImageDeprecated() {
    return imageDeprecated;
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

  public Image getImagePauseLog() {
    return imagePauseLog;
  }

  public Image getImageContinueLog() {
    return imageContinueLog;
  }

  public Image getImageHopInput() {
    return getZoomedImaged( imageHopInput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopInput() {
    return imageHopInput;
  }

  public Image getImageHopOutput() {
    return getZoomedImaged( imageHopOutput, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopOutput() {
    return imageHopOutput;
  }

  public Image getImageHopTarget() {
    return getZoomedImaged( imageHopTarget, display, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopTarget() {
    return imageHopTarget;
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
   * Loads an image from a location once. The second time, the image comes from a cache. Because of this, it's important
   * to never dispose of the image you get from here. (easy!) The images are automatically disposed when the application
   * ends.
   *
   * @param location the location of the image resource to load
   * @param width    The height to resize the image to
   * @param height   The width to resize the image to
   * @return the loaded image
   */
  public Image getImage( String location, int width, int height ) {
    Image image = imageMap.get( location );
    if ( image == null ) {
      SwtUniversalImage svg = SwtSvgImageUtil.getImage( display, location );
      int realWidth = (int) Math.round( zoomFactor * width );
      int realHeight = (int) Math.round( zoomFactor * height );
      image = new Image( display, svg.getAsBitmapForSize( display, realWidth, realHeight ), SWT.IMAGE_COPY );
      svg.dispose();
      imageMap.put( location, image );
    }
    return image;
  }

  /**
   * Loads an image from a location once. The second time, the image comes from a cache. Because of this, it's important
   * to never dispose of the image you get from here. (easy!) The images are automatically disposed when the application
   * ends.
   *
   * @param location    the location of the image resource to load
   * @param classLoader the ClassLoader to use to locate resources
   * @param width       The height to resize the image to
   * @param height      The width to resize the image to
   * @return the loaded image
   */
  public Image getImage( String location, ClassLoader classLoader, int width, int height ) {
    Image image = imageMap.get( location );
    if ( image == null ) {
      SwtUniversalImage svg = SwtSvgImageUtil.getUniversalImage( display, classLoader, location );
      image = new Image( display, getZoomedImaged( svg, display, width, height ), SWT.IMAGE_COPY );
      svg.dispose();
      imageMap.put( location, image );
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

  public Image getImageEmpty16x16() {
    return imageEmpty16x16;
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
    return imageHelpWeb;
  }

  public void setImageAddAll( Image imageAddAll ) {
    this.imageAddAll = imageAddAll;
  }

  public Image getImageAddAll() {
    return imageAddAll;
  }

  public void setImageAddSingle( Image imageAddSingle ) {
    this.imageAddSingle = imageAddSingle;
  }

  public Image getImageAddSingle() {
    return imageAddSingle;
  }

  public void setImageRemoveAll( Image imageRemoveAll ) {
    this.imageRemoveAll = imageRemoveAll;
  }

  public Image getImageRemoveAll() {
    return imageRemoveAll;
  }

  public void setImageRemoveSingle( Image imageRemoveSingle ) {
    this.imageRemoveSingle = imageRemoveSingle;
  }

  public Image getImageRemoveSingle() {
    return imageRemoveSingle;
  }

  public Image getImageBackEnabled() {
    return getZoomedImaged( imageBackEnabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageBackDisabled() {
    return getZoomedImaged( imageBackDisabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageForwardEnabled() {
    return getZoomedImaged( imageForwardEnabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageForwardDisabled() {
    return getZoomedImaged( imageForwardDisabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageRefreshEnabled() {
    return getZoomedImaged( imageRefreshEnabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageRefreshDisabled() {
    return getZoomedImaged( imageRefreshDisabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageHomeEnabled() {
    return getZoomedImaged( imageHomeEnabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageHomeDisabled() {
    return getZoomedImaged( imageHomeDisabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImagePrintEnabled() {
    return getZoomedImaged( imagePrintEnabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImagePrintDisabled() {
    return getZoomedImaged( imagePrintDisabled, display, ConstUi.DOCUMENTATION_ICON_SIZE, ConstUi.DOCUMENTATION_ICON_SIZE );
  }

  public SwtUniversalImage getDefaultArrow() {
    return defaultArrow;
  }

  public SwtUniversalImage getOkArrow() {
    return okArrow;
  }

  public SwtUniversalImage getErrorArrow() {
    return errorArrow;
  }

  public SwtUniversalImage getDisabledArrow() {
    return disabledArrow;
  }

  public SwtUniversalImage getCandidateArrow() {
    return candidateArrow;
  }

  /**
   * Gets imageToolbarBack
   *
   * @return value of imageToolbarBack
   */
  public Image getImageToolbarBack() {
    return imageToolbarBack;
  }

  /**
   * Gets imageToolbarCleanup
   *
   * @return value of imageToolbarCleanup
   */
  public Image getImageToolbarCleanup() {
    return imageToolbarCleanup;
  }

  /**
   * Gets imageToolbarClose
   *
   * @return value of imageToolbarClose
   */
  public Image getImageToolbarClose() {
    return imageToolbarClose;
  }

  /**
   * Gets imageToolbarWorkflow
   *
   * @return value of imageToolbarWorkflow
   */
  public Image getImageToolbarWorkflow() {
    return imageToolbarWorkflow;
  }

  /**
   * Gets imageToolbarPause
   *
   * @return value of imageToolbarPause
   */
  public Image getImageToolbarPause() {
    return imageToolbarPause;
  }

  /**
   * Gets imageToolbarRun
   *
   * @return value of imageToolbarRun
   */
  public Image getImageToolbarRun() {
    return imageToolbarRun;
  }

  /**
   * Gets imageToolbarRunOption
   *
   * @return value of imageToolbarRunOption
   */
  public Image getImageToolbarRunOption() {
    return imageToolbarRunOption;
  }

  /**
   * Gets imageToolbarStop
   *
   * @return value of imageToolbarStop
   */
  public Image getImageToolbarStop() {
    return imageToolbarStop;
  }

  /**
   * Gets imageToolbarPipeline
   *
   * @return value of imageToolbarPipeline
   */
  public Image getImageToolbarPipeline() {
    return imageToolbarPipeline;
  }

  /**
   * Gets imageToolbarView
   *
   * @return value of imageToolbarView
   */
  public Image getImageToolbarView() {
    return imageToolbarView;
  }

  /**
   * Gets imageToolbarViewAsXml
   *
   * @return value of imageToolbarViewAsXml
   */
  public Image getImageToolbarViewAsXml() {
    return imageToolbarViewAsXml;
  }

  /**
   * Gets imageToolbarSearch
   *
   * @return value of imageToolbarSearch
   */
  public Image getImageToolbarSearch() {
    return imageToolbarSearch;
  }

  /**
   * Gets imageToolbarSearchInactive
   *
   * @return value of imageToolbarSearchInactive
   */
  public Image getImageToolbarSearchInactive() {
    return imageToolbarSearchInactive;
  }


  /**
   * Gets imageToolbarDataOrchestration
   *
   * @return value of imageToolbarDataOrchestration
   */
  public Image getImageToolbarDataOrchestration() {
    return imageToolbarDataOrchestration;
  }

  /**
   * Gets imageToolbarDataOrchestrationInactive
   *
   * @return value of imageToolbarDataOrchestrationInactive
   */
  public Image getImageToolbarDataOrchestrationInactive() {
    return imageToolbarDataOrchestrationInactive;
  }
}
