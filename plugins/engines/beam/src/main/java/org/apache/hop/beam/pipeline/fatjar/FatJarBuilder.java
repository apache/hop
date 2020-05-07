package org.apache.hop.beam.pipeline.fatjar;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.workflow.action.IAction;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class FatJarBuilder {

  private String targetJarFile;
  private List<String> jarFiles;
  private String extraStepPluginClasses;
  private String extraXpPluginClasses;

  private transient Map<String, String> fileContentMap;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
    extraStepPluginClasses = null;
    extraXpPluginClasses = null;
  }

  public FatJarBuilder( String targetJarFile, List<String> jarFiles ) {
    this();
    this.targetJarFile = targetJarFile;
    this.jarFiles = jarFiles;
  }

  public void buildTargetJar() throws HopException {

    TransformPluginType transformPluginType = TransformPluginType.getInstance();
    ActionPluginType actionPluginType = ActionPluginType.getInstance();
    ExtensionPointPluginType xpPluginType = ExtensionPointPluginType.getInstance();

    fileContentMap = new HashMap<>();

    try {
      byte[] buffer = new byte[ 1024 ];
      ZipOutputStream zipOutputStream = new ZipOutputStream( new FileOutputStream( targetJarFile ) );

      boolean fileSystemWritten = false;
      for ( String jarFile : jarFiles ) {

        ZipInputStream zipInputStream = new ZipInputStream( new FileInputStream( jarFile ) );
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        while ( zipEntry != null ) {

          boolean skip = false;
          boolean merge = false;

          String entryName = zipEntry.getName();

          if ( entryName.contains( "META-INF/INDEX.LIST" ) ) {
            skip = true;
          }
          if ( entryName.contains( "META-INF/MANIFEST.MF" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".SF" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".DSA" ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF" ) && entryName.endsWith( ".RSA" ) ) {
            skip = true;
          }
          if ( entryName.equals( Const.XML_FILE_HOP_TRANSFORMS ) ) {
            skip = true;
          }
          if ( entryName.equals( Const.XML_FILE_HOP_EXTENSION_POINTS ) ) {
            skip = true;
          }
          if ( entryName.equals( Const.XML_FILE_HOP_WORKFLOW_ACTIONS ) ) {
            skip = true;
          }
          if ( entryName.startsWith( "META-INF/services/" ) ) {
            merge = true;
            skip = true;
          }

          if ( !skip ) {
            try {
              zipOutputStream.putNextEntry( new ZipEntry( zipEntry.getName() ) );
            } catch ( ZipException ze ) {
              // Duplicate entry!
              //
              skip = true;
            }
          }

          if ( merge ) {
            String fileContent = IOUtils.toString( zipInputStream, "UTF-8" );
            String previousContent = fileContentMap.get( entryName );
            if ( previousContent == null ) {
              fileContentMap.put( entryName, fileContent );
            } else {
              fileContentMap.put( entryName, previousContent + Const.CR + fileContent );
            }
          } else {
            int len;
            while ( ( len = zipInputStream.read( buffer ) ) > 0 ) {
              if ( !skip ) {
                zipOutputStream.write( buffer, 0, len );
              }
            }
          }

          zipInputStream.closeEntry();

          if ( !skip ) {
            zipOutputStream.closeEntry();
          }

          zipEntry = zipInputStream.getNextEntry();

        }
        zipInputStream.close();
      }

      // Add the META-INF/services files...
      //
      for ( String entryName : fileContentMap.keySet() ) {
        System.out.println( "Entry merged: " + entryName );
        String fileContent = fileContentMap.get( entryName );
        zipOutputStream.putNextEntry( new ZipEntry( entryName ) );
        zipOutputStream.write( fileContent.getBytes( "UTF-8" ) );
        zipOutputStream.closeEntry();
      }

      // Add Steps, job entries and extension point plugins in XML files
      //
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_TRANSFORMS, transformPluginType.getMainTag(), transformPluginType.getSubTag(), TransformPluginType.class, ITransformMeta.class, extraStepPluginClasses );
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_WORKFLOW_ACTIONS, actionPluginType.getMainTag(), actionPluginType.getSubTag(), ActionPluginType.class, IAction.class, null );
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_EXTENSION_POINTS, xpPluginType.getMainTag(), xpPluginType.getSubTag(), ExtensionPointPluginType.class, IExtensionPoint.class, extraXpPluginClasses );

      zipOutputStream.close();
    } catch ( Exception e ) {
      throw new HopException( "Unable to build far jar file '" + targetJarFile + "'", e );
    } finally {
      fileContentMap.clear();
    }

  }

  private void addPluginsXmlFile( ZipOutputStream zipOutputStream, String filename, String mainTag, String pluginTag, Class<? extends IPluginType> pluginTypeClass, Class<?> mainPluginClass,
                                  String extraClasses ) throws Exception {

    // Write all the internal transforms plus the selected classes...
    //
    StringBuilder xml = new StringBuilder();
    xml.append( XmlHandler.openTag( mainTag ) );
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins( pluginTypeClass );
    for ( IPlugin plugin : plugins ) {
      if ( plugin.isNativePlugin() ) {
        addPluginToXml( xml, pluginTag, plugin, mainPluginClass );
      }
    }
    if ( StringUtils.isNotEmpty( extraClasses ) ) {
      for ( String extraPluginClass : extraClasses.split( "," ) ) {
        IPlugin plugin = findPluginWithMainClass( extraPluginClass, pluginTypeClass, mainPluginClass );
        if ( plugin != null ) {
          addPluginToXml( xml, pluginTag, plugin, mainPluginClass );
        }
      }
    }

    xml.append( XmlHandler.closeTag( mainTag ) );

    zipOutputStream.putNextEntry( new ZipEntry( filename ) );
    zipOutputStream.write( xml.toString().getBytes( "UTF-8" ) );
    zipOutputStream.closeEntry();
  }

  private IPlugin findPluginWithMainClass( String extraPluginClass, Class<? extends IPluginType> pluginTypeClass, Class<?> mainClass ) {
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins( pluginTypeClass );
    for ( IPlugin plugin : plugins ) {
      String check = plugin.getClassMap().get( mainClass );
      if ( check != null && check.equals( extraPluginClass ) ) {
        return plugin;
      }
    }
    return null;

  }

  private void addPluginToXml( StringBuilder xml, String pluginTag, IPlugin plugin, Class<?> mainClass ) {
    xml.append( "<" ).append( pluginTag ).append( " id=\"" );
    xml.append( plugin.getIds()[ 0 ] );
    xml.append( "\">" );
    xml.append( XmlHandler.addTagValue( "description", plugin.getName() ) );
    xml.append( XmlHandler.addTagValue( "tooltip", plugin.getDescription() ) );
    xml.append( XmlHandler.addTagValue( "classname", plugin.getClassMap().get( mainClass ) ) );
    xml.append( XmlHandler.addTagValue( "category", plugin.getCategory() ) );
    xml.append( XmlHandler.addTagValue( "iconfile", plugin.getImageFile() ) );
    xml.append( XmlHandler.addTagValue( "documentation_url", plugin.getDocumentationUrl() ) );
    xml.append( XmlHandler.addTagValue( "cases_url", plugin.getCasesUrl() ) );
    xml.append( XmlHandler.addTagValue( "forum_url", plugin.getForumUrl() ) );
    xml.append( XmlHandler.closeTag( pluginTag ) );
  }

  public static String findPluginClasses( String pluginClassName, String pluginsToInclude ) throws HopException {
    String plugins = pluginsToInclude;

    if ( StringUtils.isEmpty( plugins ) ) {
      plugins = "kettle-beam";
    } else {
      plugins += ",kettle-beam";
    }

    Set<String> classes = new HashSet<>();
    String[] pluginFolders = plugins.split( "," );
    for ( String pluginFolder : pluginFolders ) {
      try {
        List<String> stepClasses = HopPipelineMetaToBeamPipelineConverter.findAnnotatedClasses( pluginFolder, pluginClassName );
        for ( String stepClass : stepClasses ) {
          classes.add( stepClass );
        }
      } catch ( Exception e ) {
        throw new HopException( "Error find plugin classes of annotation type '" + pluginClassName + "' in folder '" + pluginFolder, e );
      }
    }

    // OK, we now have all the classes...
    // Let's sort by name and add them in the dialog comma separated...
    //
    List<String> classesList = new ArrayList<>();
    classesList.addAll( classes );
    Collections.sort( classesList );

    StringBuffer all = new StringBuffer();
    for ( String pluginClass : classesList ) {
      if ( all.length() > 0 ) {
        all.append( "," );
      }
      all.append( pluginClass );
    }

    return all.toString();
  }


  /**
   * Gets targetJarFile
   *
   * @return value of targetJarFile
   */
  public String getTargetJarFile() {
    return targetJarFile;
  }

  /**
   * @param targetJarFile The targetJarFile to set
   */
  public void setTargetJarFile( String targetJarFile ) {
    this.targetJarFile = targetJarFile;
  }

  /**
   * Gets jarFiles
   *
   * @return value of jarFiles
   */
  public List<String> getJarFiles() {
    return jarFiles;
  }

  /**
   * @param jarFiles The jarFiles to set
   */
  public void setJarFiles( List<String> jarFiles ) {
    this.jarFiles = jarFiles;
  }

  /**
   * Gets extraStepPluginClasses
   *
   * @return value of extraStepPluginClasses
   */
  public String getExtraStepPluginClasses() {
    return extraStepPluginClasses;
  }

  /**
   * @param extraStepPluginClasses The extraStepPluginClasses to set
   */
  public void setExtraStepPluginClasses( String extraStepPluginClasses ) {
    this.extraStepPluginClasses = extraStepPluginClasses;
  }

  /**
   * Gets extraXpPluginClasses
   *
   * @return value of extraXpPluginClasses
   */
  public String getExtraXpPluginClasses() {
    return extraXpPluginClasses;
  }

  /**
   * @param extraXpPluginClasses The extraXpPluginClasses to set
   */
  public void setExtraXpPluginClasses( String extraXpPluginClasses ) {
    this.extraXpPluginClasses = extraXpPluginClasses;
  }

  /**
   * Gets fileContentMap
   *
   * @return value of fileContentMap
   */
  public Map<String, String> getFileContentMap() {
    return fileContentMap;
  }

  /**
   * @param fileContentMap The fileContentMap to set
   */
  public void setFileContentMap( Map<String, String> fileContentMap ) {
    this.fileContentMap = fileContentMap;
  }
}
