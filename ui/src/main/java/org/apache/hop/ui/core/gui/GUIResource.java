//CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.util.ImageUtil;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
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
public class GUIResource {

  private static ILogChannel log = new LogChannel( "GUIResource" );

  private static GUIResource guiResource;

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

  private Map<String, SwtUniversalImage> imagesJobentries;

  private Map<String, Image> imagesJobentriesSmall;

  private SwtUniversalImage imageHop;

  private SwtUniversalImage imageDisabledHop;

  private SwtUniversalImage imageConnection;

  private SwtUniversalImage imageConnectionTree;

  private Image imageAdd;

  private Image imageTable;

  private SwtUniversalImage imagePreview;

  private Image imageKeySmall;

  private Image imageColumnSmall;

  private Image imageSchema;

  private Image imageSynonym;

  private Image imageProcedure;

  private Image imageExploreDbSmall;

  private Image imageView;

  private Image imageHopLogo;

  private SwtUniversalImage imageLogoSmall;

  private Image imageBanner;

  private SwtUniversalImage imageBol;

  private Image imageCalendar;

  private SwtUniversalImage imageCluster;

  private SwtUniversalImage imageSlave;

  private SwtUniversalImage imageSlaveTree;

  private SwtUniversalImage imageArrow;

  private SwtUniversalImage imageFolder;

  private Image imageWizard;

  private Image imageCredits;

  private SwtUniversalImage imageStart;

  private SwtUniversalImage imageDummy;

  private SwtUniversalImage imageMissing;

  private SwtUniversalImage imageHopUi;

  private Image imageJob;

  private SwtUniversalImage imageVariable;

  private SwtUniversalImage imagePipelineGraph;

  private SwtUniversalImage imagePartitionSchema;

  private SwtUniversalImage imageJobGraph;

  private SwtUniversalImage imagePipelineTree;

  private SwtUniversalImage imageJobTree;

  private SwtUniversalImage defaultArrow;
  private SwtUniversalImage okArrow;
  private SwtUniversalImage errorArrow;
  private SwtUniversalImage disabledArrow;
  private SwtUniversalImage candidateArrow;

  private Image imageUser;

  private Image imageProfil;

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

  private Image imageDesignPanel;

  private Image imageViewPanel;

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

  private Image imageVersionBrowser;

  private Image imageDeprecated;

  private Image imageNew;

  private SwtUniversalImage imageEdit;

  private Image imageDelete;

  private Image imageShowDeleted;

  private Image imagePauseLog;

  private Image imageContinueLog;

  private Image imageImport;

  private Image imageExport;

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

  private Image imageGantt;

  private Image imageHelpWeb;

  /**
   * Same result as <code>new Image(display, 16, 16)</code>.
   */
  private Image imageEmpty16x16;

  private Map<String, Image> imageMap;

  private Map<RGB, Color> colorMap;

  private Image imageHadoop;

  private Image imageDropHere;

  private Image imagePipelineCanvas;

  private Image imageJobCanvas;

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
  private Image imageToolbarJob;
  private Image imageToolbarPause;
  private Image imageToolbarRun;
  private Image imageToolbarRunOption;
  private Image imageToolbarStop;
  private Image imageToolbarView;
  private Image imageToolbarViewAsXml;

  private Image imageToolbarPipeline;
  private Image imageToolbarPipelineInactive;

  private Image imageToolbarSearch;
  private Image imageToolbarSearchInactive;

  /**
   * GUIResource also contains the clipboard as it has to be allocated only once! I don't want to put it in a separate
   * singleton just for this one member.
   */
  private static Clipboard clipboard;

  private GUIResource( Display display ) {
    this.display = display;

    getResources();

    display.addListener( SWT.Dispose, new Listener() {
      public void handleEvent( Event event ) {
        dispose( false );
      }
    } );

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

    PluginRegistry.getInstance().addPluginListener( JobEntryPluginType.class, new IPluginTypeListener() {
      @Override public void pluginAdded( Object serviceObject ) {
        // make sure we load up the images for any new job entries that have been registered
        loadJobEntryImages();
      }

      @Override public void pluginRemoved( Object serviceObject ) {
        // rebuild the image map, in effect removing the image(s) for job entries that have gone away
        loadJobEntryImages();
      }

      @Override public void pluginChanged( Object serviceObject ) {
        // nothing needed here
      }
    } );

  }

  public static final GUIResource getInstance() {
    if ( guiResource != null ) {
      return guiResource;
    }
    guiResource = new GUIResource( PropsUI.getDisplay() );
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
    PropsUI props = PropsUI.getInstance();
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
    loadJobEntryImages();
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
      imageConnectionTree.dispose();
      imageAdd.dispose();
      imageTable.dispose();
      imagePreview.dispose();
      imageSchema.dispose();
      imageSynonym.dispose();
      imageProcedure.dispose();
      imageExploreDbSmall.dispose();
      imageView.dispose();
      imageLogoSmall.dispose();
      imageHopLogo.dispose();
      imageBanner.dispose();
      imageBol.dispose();
      imageCalendar.dispose();
      imageCluster.dispose();
      imageSlave.dispose();
      imageSlaveTree.dispose();
      imageArrow.dispose();
      imageFolder.dispose();
      imageWizard.dispose();
      imageCredits.dispose();
      imageStart.dispose();
      imageDummy.dispose();
      imageMissing.dispose();
      imageHopUi.dispose();
      imageJob.dispose();
      imageVariable.dispose();
      imagePipelineGraph.dispose();
      imagePartitionSchema.dispose();
      imageJobGraph.dispose();
      imagePipelineTree.dispose();
      imageJobTree.dispose();
      imageUser.dispose();
      imageProfil.dispose();
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
      imageVersionBrowser.dispose();
      imageClearText.dispose();
      imageDeprecated.dispose();
      imageClearTextDisabled.dispose();
      imageExpandAll.dispose();
      imageSearchSmall.dispose();
      imageRegExSmall.dispose();
      imageViewPanel.dispose();
      imageDesignPanel.dispose();
      imageNew.dispose();
      imageEdit.dispose();
      imageDelete.dispose();
      imageShowDeleted.dispose();
      imagePauseLog.dispose();
      imageContinueLog.dispose();
      imageLocked.dispose();
      imageImport.dispose();
      imageExport.dispose();
      imageHopInput.dispose();
      imageHopOutput.dispose();
      imageHopTarget.dispose();
      imageKeySmall.dispose();
      imageColumnSmall.dispose();
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
      imageGantt.dispose();
      imageHelpWeb.dispose();
      imageHadoop.dispose();
      imageDropHere.dispose();
      imagePipelineCanvas.dispose();
      imageJobCanvas.dispose();
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
      imageToolbarJob.dispose();
      imageToolbarPause.dispose();
      imageToolbarRun.dispose();
      imageToolbarRunOption.dispose();
      imageToolbarStop.dispose();
      imageToolbarPipeline.dispose();
      imageToolbarPipelineInactive.dispose();
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
      smallImage = image.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );

      imagesTransforms.put( transform.getIds()[ 0 ], image );
      imagesTransformsSmall.put( transform.getIds()[ 0 ], smallImage );
    }
  }

  private void loadFonts() {
    PropsUI props = PropsUI.getInstance();

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
    FontData largeFontData =new FontData( props.getGraphFont().getName(), props.getGraphFont().getHeight() * 3, props.getGraphFont().getStyle() );
    fontLarge = new ManagedFont( display, largeFontData );

    // Create a tiny version of the graph font
    FontData tinyFontData = new FontData( props.getGraphFont().getName(), props.getGraphFont().getHeight() - 2, props.getGraphFont().getStyle() );
    fontTiny = new ManagedFont( display, tinyFontData );

    // Create a small version of the graph font
    FontData smallFontData = new FontData( props.getGraphFont().getName(), props.getGraphFont().getHeight() - 1, props.getGraphFont().getStyle() );
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

    // "ui/images/CNC.png"
    imageConnection = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CNC_image" ) );

    // "ui/images/CNC_tree"
    imageConnectionTree = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CNC_tree_image" ) );

    // "ui/images/Add.png"
    imageAdd = loadAsResource( display, BasePropertyHandler.getProperty( "Add_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/table.png"
    imageTable = loadAsResource( display, BasePropertyHandler.getProperty( "Table_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/preview.svg"
    imagePreview = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Preview_image" ) );

    // "ui/images/schema.png"
    imageSchema = loadAsResource( display, BasePropertyHandler.getProperty( "Schema_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/synonym.png"
    imageSynonym = loadAsResource( display, BasePropertyHandler.getProperty( "Synonym_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/view.png"
    imageView = loadAsResource( display, BasePropertyHandler.getProperty( "View_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/color.png.png"
    imageColor = loadAsResource( display, BasePropertyHandler.getProperty( "Color_image" ), 12 );

    // "ui/images/noteSmall.png"
    imageNoteSmall = loadAsResource( display, BasePropertyHandler.getProperty( "Note_image" ), 12 );

    // "ui/images/proc.png"
    imageProcedure = loadAsResource( display, BasePropertyHandler.getProperty( "ProcedureSmall_image" ), ConstUI.SMALL_ICON_SIZE );

    // , "ui/images/exploreDbSmall.png"
    imageExploreDbSmall =
      loadAsResource( display, BasePropertyHandler.getProperty( "ExploreDbSmall_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/cluster.png"
    imageCluster = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Cluster_image" ) );

    // , "ui/images/slave.png"
    imageSlave = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Slave_image" ) );

    // , "ui/images/slave-tree.png"
    imageSlaveTree = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Slave_tree_image" ) );

    // "ui/images/logo_kettle_lrg.png"
    imageHopLogo = loadAsResource( display, BasePropertyHandler.getProperty( "Logo_lrg_image" ), 0 );
    // "ui/images/bg_banner.png"
    imageBanner = loadAsResource( display, BasePropertyHandler.getProperty( "Banner_bg_image" ), 0 );

    // "ui/images/BOL.png"
    imageBol = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BOL_image" ) );

    // "ui/images/Calendar.png"
    imageCalendar = loadAsResource( display, BasePropertyHandler.getProperty( "Calendar_image" ), ConstUI.SMALL_ICON_SIZE ); // ,

    // "ui/images/credits.png"
    imageCredits = loadAsResource( display, BasePropertyHandler.getProperty( "Credits_image" ), 0 );

    // "ui/images/STR.png"
    imageStart = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "STR_image" ) );

    // "ui/images/DUM.png"
    imageDummy = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "DUM_image" ) );

    //ui/images/missing_entry.svg
    imageMissing = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "MIS_image" ) );

    // "ui/images/hop-logo.svg"
    imageHopUi = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "hop_icon" ) );

    // "ui/images/chefgraph.svg"
    imageJob = loadAsResource( display, BasePropertyHandler.getProperty( "Job_image" ), ConstUI.ICON_SIZE );

    // "ui/images/variable.svg"
    imageVariable = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Variable_image" ) );

    // "ui/images/edit_option.png"
    imageEditOptionButton = loadAsResource( display, BasePropertyHandler.getProperty( "EditOption_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/reset_option.png"
    imageResetOptionButton = loadAsResource( display, BasePropertyHandler.getProperty( "ResetOption_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/Edit.png"
    imageEditSmall = loadAsResource( display, BasePropertyHandler.getProperty( "EditSmall_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/exploreSolution.png"
    imageExploreSolutionSmall = loadAsResource( display, BasePropertyHandler.getProperty( "ExploreSolutionSmall_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-log.png"
    imageShowLog = loadAsResource( display, BasePropertyHandler.getProperty( "ShowLog_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-grid.png"
    imageShowGrid = loadAsResource( display, BasePropertyHandler.getProperty( "ShowGrid_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-history.png"
    imageShowHistory = loadAsResource( display, BasePropertyHandler.getProperty( "ShowHistory_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-perf.png"
    imageShowPerf = loadAsResource( display, BasePropertyHandler.getProperty( "ShowPerf_image" ), ConstUI.SMALL_ICON_SIZE );

    // ui/images/show-inactive-selected.png
    imageShowInactive = loadAsResource( display, BasePropertyHandler.getProperty( "ShowInactive_image" ), ConstUI.SMALL_ICON_SIZE );

    // ui/images/show-inactive-selected.png
    imageHideInactive = loadAsResource( display, BasePropertyHandler.getProperty( "HideInactive_image" ), ConstUI.SMALL_ICON_SIZE );

    // ui/images/show-selected.png
    imageShowSelected = loadAsResource( display, BasePropertyHandler.getProperty( "ShowSelected_image" ), ConstUI.SMALL_ICON_SIZE );

    // ui/images/show-all.png
    imageShowAll = loadAsResource( display, BasePropertyHandler.getProperty( "ShowAll_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-perf.png"
    imageClosePanel = loadAsResource( display, BasePropertyHandler.getProperty( "ClosePanel_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-perf.png"
    imageMaximizePanel = loadAsResource( display, BasePropertyHandler.getProperty( "MaximizePanel_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-perf.png"
    imageMinimizePanel = loadAsResource( display, BasePropertyHandler.getProperty( "MinimizePanel_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-perf.png"
    imageShowErrorLines = loadAsResource( display, BasePropertyHandler.getProperty( "ShowErrorLines_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-results.png
    imageShowResults = loadAsResource( display, BasePropertyHandler.getProperty( "ShowResults_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/hide-results.png
    imageHideResults = loadAsResource( display, BasePropertyHandler.getProperty( "HideResults_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/Design.png;
    imageDesignPanel = loadAsResource( display, BasePropertyHandler.getProperty( "DesignPanel_image" ), 0 );

    // "ui/images/View.png;
    imageViewPanel = loadAsResource( display, BasePropertyHandler.getProperty( "ViewPanel_image" ), 0 );

    // "ui/images/ClearText.png;
    imageClearText = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ClearText_image" ) );

    // "ui/images/ClearTextDisabled.png;
    imageClearTextDisabled = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ClearTextDisabled_image" ) );

    // "ui/images/ExpandAll.png;
    imageExpandAll = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ExpandAll_image" ) );

    // "ui/images/CollapseAll.png;
    imageCollapseAll = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CollapseAll_image" ) );

    // "ui/images/show-error-lines.png;
    imageTransformError = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "TransformErrorLines_image" ) );

    // "ui/images/transform-error.svg;
    imageRedTransformError = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "TransformErrorLinesRed_image" ) );

    // "ui/images/copy-hop.png;
    imageCopyHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CopyHop_image" ) );

    // "ui/images/error-hop.png;
    imageErrorHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ErrorHop_image" ) );

    // "ui/images/info-hop.png;
    imageInfoHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "InfoHop_image" ) );

    // "ui/images/warning.png;
    imageWarning = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Warning_image" ) );

    // "ui/images/deprecated.svg
    imageDeprecated = loadAsResource( display, BasePropertyHandler.getProperty( "Deprecated_image" ),
      ConstUI.LARGE_ICON_SIZE );

    // "ui/images/version-history.png;
    imageVersionBrowser = loadAsResource( display, BasePropertyHandler.getProperty( "VersionBrowser_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/generic-new.png;
    imageNew = loadAsResource( display, BasePropertyHandler.getProperty( "Add_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/generic-edit.png;
    imageEdit = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "EditSmall_image" ) );

    // "ui/images/generic-delete.png;
    imageDelete = loadAsResource( display, BasePropertyHandler.getProperty( "DeleteOriginal_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/show-deleted.png;
    imageShowDeleted = loadAsResource( display, BasePropertyHandler.getProperty( "ShowDeleted_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/pause-log.png;
    imagePauseLog = loadAsResource( display, BasePropertyHandler.getProperty( "PauseLog_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/continue-log.png;
    imageContinueLog = loadAsResource( display, BasePropertyHandler.getProperty( "ContinueLog_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/import.png;
    imageImport = loadAsResource( display, BasePropertyHandler.getProperty( "Import_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/export.png;
    imageExport = loadAsResource( display, BasePropertyHandler.getProperty( "Export_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/hop-input.png;
    imageHopInput = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopInput_image" ) );

    // "ui/images/hop-output.png;
    imageHopOutput = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopOutput_image" ) );

    // "ui/images/hop-target.png;
    imageHopTarget = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "HopTarget_image" ) );

    // "ui/images/locked.png;
    imageLocked = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Locked_image" ) );

    // "ui/images/true.png;
    imageTrue = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "True_image" ) );

    // "ui/images/false.png;
    imageFalse = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "False_image" ) );

    // "ui/images/context_menu.png;
    imageContextMenu = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ContextMenu_image" ) );

    // "ui/images/parallel-hop.png
    imageParallelHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "ParallelHop_image" ) );

    // "ui/images/unconditional-hop.png
    imageUnconditionalHop = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "UnconditionalHop_image" ) );

    // "ui/images/busy.png
    imageBusy = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Busy_image" ) );

    // "ui/images/inject.png
    imageInject = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "Inject_image" ) );

    // "ui/images/scales.png
    imageBalance = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "LoadBalance_image" ) );

    // "ui/images/scales.png
    imageCheckpoint = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "CheckeredFlag_image" ) );

    // "ui/images/gantt.png
    imageGantt = loadAsResource( display, BasePropertyHandler.getProperty( "Gantt_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/help_web.png
    imageHelpWeb = loadAsResource( display, BasePropertyHandler.getProperty( "HelpWeb_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/hadoop.png
    imageHadoop = loadAsResource( display, BasePropertyHandler.getProperty( "Hadoop_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/drop_here.png
    imageDropHere = loadAsResource( display, BasePropertyHandler.getProperty( "DropHere_image" ), 0 );

    // "ui/images/pipeline_canvas.svg
    imagePipelineCanvas = loadAsResource( display, BasePropertyHandler.getProperty( "PipelineCanvas_image" ), 800, 250 );

    // "ui/images/job_canvas.svg
    imageJobCanvas = loadAsResource( display, BasePropertyHandler.getProperty( "JobCanvas_image" ), 800, 250 );

    // "ui/images/add_all.png
    imageAddAll = loadAsResource( display, BasePropertyHandler.getProperty( "AddAll_image" ), 12 );

    // "ui/images/add_single.png
    imageAddSingle = loadAsResource( display, BasePropertyHandler.getProperty( "AddSingle_image" ), 12 );

    // "ui/images/remove_all.png
    imageRemoveAll = loadAsResource( display, BasePropertyHandler.getProperty( "RemoveAll_image" ), 12 );

    // "ui/images/remove_single.png
    imageRemoveSingle = loadAsResource( display, BasePropertyHandler.getProperty( "RemoveSingle_image" ), 12 );

    // ui/images/back-enabled.png
    imageBackEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BackEnabled" ) );

    // ui/images/back-disabled.png
    imageBackDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "BackDisabled" ) );

    // ui/images/forward-enabled.png
    imageForwardEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ForwardEnabled" ) );

    // ui/images/forward-disabled.png
    imageForwardDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ForwardDisabled" ) );

    // ui/images/refresh-enabled.png
    imageRefreshEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "RefreshEnabled" ) );

    // ui/images/refresh-disabled.png
    imageRefreshDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "RefreshDisabled" ) );

    // ui/images/home-enabled.png
    imageHomeEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "HomeEnabled" ) );

    // ui/images/home-disabled.png
    imageHomeDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "HomeDisabled" ) );

    // ui/images/print-enabled.png
    imagePrintEnabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PrintEnabled" ) );

    // ui/images/print-disabled.png
    imagePrintDisabled = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PrintDisabled" ) );

    imageEmpty16x16 = new Image( display, 16, 16 );

    imagePipelineGraph = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "PipelineGraph_image" ) );

    imagePartitionSchema = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Image_Partition_Schema" ) );

    imageJobGraph = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ChefIcon_image" ) );

    imagePipelineTree = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Pipeline_tree_image" ) );
    imageJobTree = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Job_tree_image" ) );

    // "ui/images/hop-logo.png"
    imageLogoSmall = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Logo_sml_image" ) );

    // "ui/images/arrow.png"
    imageArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "ArrowIcon_image" ) );

    // "ui/images/folder.png"
    imageFolder = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "Folder_image" ) );

    // Makes transparent images "on the fly"
    //

    // "ui/images/wizard.png"
    imageWizard = loadAsResource( display, BasePropertyHandler.getProperty( "spoon_icon" ), 0 );

    // , "ui/images/bg_banner.png"
    imageBanner = ImageUtil.makeImageTransparent( display, loadAsResource( display, BasePropertyHandler.getProperty( "Banner_bg_image" ), 0 ), new RGB( 255, 255, 255 ) );

    // , "ui/images/user.png"
    imageUser = loadAsResource( display, BasePropertyHandler.getProperty( "User_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/profil.png"
    imageProfil = loadAsResource( display, BasePropertyHandler.getProperty( "Profil_image" ), ConstUI.SMALL_ICON_SIZE );

    // "ui/images/folder_connection.png"
    imageFolderConnections = SwtSvgImageUtil.getImageAsResource( display, BasePropertyHandler.getProperty( "FolderConnections_image" ) );

    imageRegExSmall = loadAsResource( display, BasePropertyHandler.getProperty( "RegExSmall_image" ), ConstUI.SMALL_ICON_SIZE );
    imageSearchSmall = loadAsResource( display, BasePropertyHandler.getProperty( "SearchSmall_image" ), ConstUI.SMALL_ICON_SIZE );
    imageKeySmall = loadAsResource( display, BasePropertyHandler.getProperty( "KeySmall_image" ), ConstUI.SMALL_ICON_SIZE );
    imageColumnSmall = loadAsResource( display, BasePropertyHandler.getProperty( "ColumnSmall_image" ), ConstUI.SMALL_ICON_SIZE );

    defaultArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "defaultArrow_image" ) );
    okArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "okArrow_image" ) );
    errorArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "errorArrow_image" ) );
    disabledArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "disabledArrow_image" ) );
    candidateArrow = SwtSvgImageUtil.getUniversalImage( display, getClass().getClassLoader(), BasePropertyHandler.getProperty( "candidateArrow_image" ) );


    // Toolbar icons
    //
    imageToolbarBack = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarBack_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarCleanup = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarCleanup_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarClose = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarClose_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarJob = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarJob_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarPause = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarPause_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarRun = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarRun_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarRunOption = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarRunOption_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarStop = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarStop_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarPipeline = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarPipeline_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarPipelineInactive = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarPipelineInactive_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarSearch = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarSearch_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarSearchInactive = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarSearchInactive_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarView = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarView_image" ), ConstUI.SMALL_ICON_SIZE );
    imageToolbarViewAsXml = loadAsResource( display, BasePropertyHandler.getProperty( "toolbarViewAsXml_image" ), ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * Load all transform images from files.
   */
  private void loadJobEntryImages() {
    imagesJobentries = new Hashtable<String, SwtUniversalImage>();
    imagesJobentriesSmall = new Hashtable<String, Image>();

    // //
    // // JOB ENTRY IMAGES TO LOAD
    // //
    PluginRegistry registry = PluginRegistry.getInstance();

    List<IPlugin> plugins = registry.getPlugins( JobEntryPluginType.class );
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
        small_image = image.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
      }

      imagesJobentries.put( plugin.getIds()[ 0 ], image );
      imagesJobentriesSmall.put( plugin.getIds()[ 0 ], small_image );
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
   * @return Returns the display.
   */
  public Display getDisplay() {
    return display;
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
    return imageBol.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageCalendar.
   */
  public Image getImageCalendar() {
    return imageCalendar;
  }

  /**
   * @return Returns the imageCluster.
   */
  public Image getImageCluster() {
    return imageCluster.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageClusterMedium() {
    return imageCluster.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageSlave.
   */
  public Image getImageSlave() {
    return imageSlave.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageSlave.
   */
  public Image getImageSlaveMedium() {
    return imageSlave.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageSlaveTree.
   */
  public Image getImageSlaveTree() {
    return imageSlaveTree.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageSlave.
   */
  public Image getImageSlaveTreeMedium() {
    return imageSlaveTree.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageConnection.
   */
  public Image getImageConnection() {
    return imageConnection.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageConnectionTree() {
    return imageConnectionTree.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageConnection() {
    return imageConnection;
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
    return imagePreview.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageKeySmall.
   */
  public Image getImageKeySmall() {
    return imageKeySmall;
  }

  /**
   * @return Returns the imageColumnSmall.
   */
  public Image getImageColumnSmall() {
    return imageColumnSmall;
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
   * @return Returns the imageProcedure.
   */
  public Image getImageProcedure() {
    return imageProcedure;
  }

  /**
   * @return Returns the imageExploreDbSmall.
   */
  public Image getImageExploreDbSmall() {
    return imageExploreDbSmall;
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
   * @return Returns the imageCredits.
   */
  public Image getImageCredits() {
    return imageCredits;
  }

  /**
   * @return Returns the imageDummy.
   */
  public Image getImageDummy() {
    return imageDummy.getAsBitmapForSize( display, ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageDummy() {
    return imageDummy;
  }

  /**
   * @return Returns the imageMissing.
   */
  public Image getImageMissing() {
    return imageMissing.getAsBitmapForSize( display, ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageMissing() {
    return imageMissing;
  }

  /**
   * @return Returns the imageHop.
   */
  public Image getImageHop() {
    return imageHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageDisabledHop.
   */
  public Image getImageDisabledHop() {
    return imageDisabledHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return Returns the imageHop.
   */
  public Image getImageHopTree() {
    return imageHop.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageDisabledHop.
   */
  public Image getImageDisabledHopTree() {
    return imageDisabledHop.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return Returns the imageHopGui.
   */
  public Image getImageHopUi() {
    return imageHopUi.getAsBitmapForSize( display, ConstUI.LARGE_ICON_SIZE, ConstUI.LARGE_ICON_SIZE );
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
    return imageStart.getAsBitmapForSize( display, ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageStart() {
    return imageStart;
  }

  /**
   * @return Returns the imagesJobentries.
   */
  public Map<String, SwtUniversalImage> getImagesJobentries() {
    return imagesJobentries;
  }

  /**
   * @param imagesJobentries The imagesJobentries to set.
   */
  public void setImagesJobentries( Hashtable<String, SwtUniversalImage> imagesJobentries ) {
    this.imagesJobentries = imagesJobentries;
  }

  /**
   * @return Returns the imagesJobentriesSmall.
   */
  public Map<String, Image> getImagesJobentriesSmall() {
    return imagesJobentriesSmall;
  }

  /**
   * @param imagesJobentriesSmall The imagesJobentriesSmall to set.
   */
  public void setImagesJobentriesSmall( Hashtable<String, Image> imagesJobentriesSmall ) {
    this.imagesJobentriesSmall = imagesJobentriesSmall;
  }

  /**
   * @return Returns the imageChef.
   */
  public Image getImageChef() {
    return imageJob;
  }

  /**
   * @param imageChef The imageChef to set.
   */
  public void setImageChef( Image imageChef ) {
    this.imageJob = imageChef;
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

  /**
   * @return the imageVariable
   */
  public Image getImageVariable() {
    return imageVariable.getAsBitmapForSize( display, 13, 13 );
  }

  public Image getImagePipelineGraph() {
    return imagePipelineGraph.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImagePipelineTree() {
    return imagePipelineTree.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public Image getImageUser() {
    return imageUser;
  }

  public Image getImageProfil() {
    return imageProfil;
  }

  public Image getImageFolderConnections() {
    return imagePipelineGraph.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageFolderConnectionsMedium() {
    return imagePipelineGraph.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public Image getImagePartitionSchema() {
    return imagePartitionSchema.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public Image getImageJobGraph() {
    return imageJobGraph.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageJobTree() {
    return imageJobTree.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
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
    return imageArrow.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageArrow() {
    return imageArrow;
  }

  /**
   * @return the imageArrow
   */
  public Image getImageFolder() {
    return imageFolder.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageDummySmall
   */
  public Image getImageDummySmall() {
    return imageDummy.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return the imageStartSmall
   */
  public Image getImageStartSmall() {
    return imageStart.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  /**
   * @return the imageDummyMedium
   */
  public Image getImageDummyMedium() {
    return imageDummy.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageStartSmall
   */
  public Image getImageStartMedium() {
    return imageStart.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  /**
   * @return the imageBanner
   */
  public Image getImageBanner() {
    return imageBanner;
  }

  /**
   * @return the imageWizard
   */
  public Image getImageWizard() {
    return imageWizard;
  }

  /**
   * @param imageBanner the imageBanner to set
   */
  public void setImageBanner( Image imageBanner ) {
    this.imageBanner = imageBanner;
  }

  /**
   * @return the imageHopLogo
   */
  public Image getImageHopLogo() {
    return imageHopLogo;
  }

  /**
   * @param imageHopLogo the imageHopLogo to set
   */
  public void setImageHopLogo( Image imageHopLogo ) {
    this.imageHopLogo = imageHopLogo;
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
    return imageLogoSmall.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
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
      gc.setBackground( GUIResource.getInstance().getColorHop() );
      gc.fillGradientRectangle( rect.x, rect.y, 2 * rect.width / 3, rect.height, vertical );
      gc.setForeground( GUIResource.getInstance().getColorHop() );
      gc.setBackground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.fillGradientRectangle( rect.x + 2 * rect.width / 3, rect.y, rect.width / 3 + 1, rect.height, vertical );
    } else {
      gc.setForeground( display.getSystemColor( SWT.COLOR_WIDGET_BACKGROUND ) );
      gc.setBackground( GUIResource.getInstance().getColorHop() );
      gc.fillGradientRectangle( rect.x, rect.y, rect.width, 2 * rect.height / 3, vertical );
      gc.setForeground( GUIResource.getInstance().getColorHop() );
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

  public Image getImageDesignPanel() {
    return imageDesignPanel;
  }

  public Image getImageViewPanel() {
    return imageViewPanel;
  }

  public Image getImageClearText() {
    return imageClearText.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageClearTextDisabled() {
    return imageClearTextDisabled.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageExpandAll() {
    return imageExpandAll.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageExpandAllMedium() {
    return imageExpandAll.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public Image getImageSearchSmall() {
    return imageSearchSmall;
  }

  public Image getImageRegexSmall() {
    return imageRegExSmall;
  }

  public Image getImageCollapseAll() {
    return imageCollapseAll.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageCollapseAllMedium() {
    return imageCollapseAll.getAsBitmapForSize( display, ConstUI.MEDIUM_ICON_SIZE, ConstUI.MEDIUM_ICON_SIZE );
  }

  public Image getImageTransformError() {
    return imageTransformError.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageTransformError() {
    return imageTransformError;
  }

  public Image getImageRedTransformError() {
    return imageRedTransformError.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageRedTransformError() {
    return imageRedTransformError;
  }

  public Image getImageCopyHop() {
    return imageCopyHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageCopyHop() {
    return imageCopyHop;
  }

  public Image getImageErrorHop() {
    return imageErrorHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageErrorHop() {
    return imageErrorHop;
  }

  public Image getImageInfoHop() {
    return imageInfoHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInfoHop() {
    return imageInfoHop;
  }

  public Image getImageWarning() {
    return imageWarning.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public Image getImageWarning32() {
    return imageWarning.getAsBitmapForSize( display, ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageWarning() {
    return imageWarning;
  }

  public Image getImageVersionBrowser() {
    return imageVersionBrowser;
  }

  public Image getImageDeprecated() {
    return imageDeprecated;
  }

  public Image getImageNew() {
    return imageNew;
  }

  public Image getImageEdit() {
    return imageEdit.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageEdit() {
    return imageEdit;
  }

  public Image getImageDelete() {
    return imageDelete;
  }

  public Image getImageShowDeleted() {
    return imageShowDeleted;
  }

  public Image getImagePauseLog() {
    return imagePauseLog;
  }

  public Image getImageContinueLog() {
    return imageContinueLog;
  }

  public Image getImageImport() {
    return imageImport;
  }

  public Image getImageExport() {
    return imageExport;
  }

  public Image getImageHopInput() {
    return imageHopInput.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopInput() {
    return imageHopInput;
  }

  public Image getImageHopOutput() {
    return imageHopOutput.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopOutput() {
    return imageHopOutput;
  }

  public Image getImageHopTarget() {
    return imageHopTarget.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageHopTarget() {
    return imageHopTarget;
  }

  public Image getImageLocked() {
    return imageLocked.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
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
    return getImage( location, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
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
      image = new Image( display, svg.getAsBitmapForSize( display, width, height ), SWT.IMAGE_COPY );
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
    return imageTrue.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageTrue() {
    return imageTrue;
  }

  /**
   * @return the imageFalse
   */
  public Image getImageFalse() {
    return imageFalse.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageFalse() {
    return imageFalse;
  }

  /**
   * @return the imageContextMenu
   */
  public Image getImageContextMenu() {
    return imageContextMenu.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageContextMenu() {
    return imageContextMenu;
  }

  public Image getImageParallelHop() {
    return imageParallelHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageParallelHop() {
    return imageParallelHop;
  }

  public Image getImageUnconditionalHop() {
    return imageUnconditionalHop.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageUnconditionalHop() {
    return imageUnconditionalHop;
  }

  public Image getImageBusy() {
    return imageBusy.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageBusy() {
    return imageBusy;
  }

  public Image getImageEmpty16x16() {
    return imageEmpty16x16;
  }

  public Image getImageInject() {
    return imageInject.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageInject() {
    return imageInject;
  }

  public Image getImageBalance() {
    return imageBalance.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageBalance() {
    return imageBalance;
  }

  public Image getImageCheckpoint() {
    return imageCheckpoint.getAsBitmapForSize( display, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  public SwtUniversalImage getSwtImageCheckpoint() {
    return imageCheckpoint;
  }

  public Image getImageGantt() {
    return imageGantt;
  }

  public Image getImageHelpWeb() {
    return imageHelpWeb;
  }

  public Image getHadoop() {
    return imageHadoop;
  }

  public void setImageDropHere( Image imageDropHere ) {
    this.imageDropHere = imageDropHere;
  }

  public Image getImageDropHere() {
    return imageDropHere;
  }

  public Image getImagePipelineCanvas() {
    return imagePipelineCanvas;
  }

  public void setImagePipelineCanvas( Image imagePipelineCanvas ) {
    this.imagePipelineCanvas = imagePipelineCanvas;
  }

  public Image getImageJobCanvas() {
    return imageJobCanvas;
  }

  public void setImageJobCanvas( Image imageJobCanvas ) {
    this.imageJobCanvas = imageJobCanvas;
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
    return imageBackEnabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageBackDisabled() {
    return imageBackDisabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageForwardEnabled() {
    return imageForwardEnabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageForwardDisabled() {
    return imageForwardDisabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageRefreshEnabled() {
    return imageRefreshEnabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageRefreshDisabled() {
    return imageRefreshDisabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageHomeEnabled() {
    return imageHomeEnabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImageHomeDisabled() {
    return imageHomeDisabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImagePrintEnabled() {
    return imagePrintEnabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
  }

  public Image getImagePrintDisabled() {
    return imagePrintDisabled.getAsBitmapForSize( display, ConstUI.DOCUMENTATION_ICON_SIZE,
      ConstUI.DOCUMENTATION_ICON_SIZE );
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
   * Gets imageToolbarJob
   *
   * @return value of imageToolbarJob
   */
  public Image getImageToolbarJob() {
    return imageToolbarJob;
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
   * Gets imageToolbarPipelineInactive
   *
   * @return value of imageToolbarPipelineInactive
   */
  public Image getImageToolbarPipelineInactive() {
    return imageToolbarPipelineInactive;
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
}
