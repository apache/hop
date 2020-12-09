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

package org.apache.hop.beam.pipeline.fatjar;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchableAnalyserPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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

  private IVariables variables;
  private String targetJarFile;
  private List<String> jarFiles;
  private String extraTransformPluginClasses;
  private String extraXpPluginClasses;

  private transient Map<String, String> fileContentMap;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
    extraTransformPluginClasses = null;
    extraXpPluginClasses = null;
  }

  public FatJarBuilder( IVariables variables, String targetJarFile, List<String> jarFiles ) {
    this();
    this.variables = variables;
    this.targetJarFile = targetJarFile;
    this.jarFiles = jarFiles;
  }

  public void buildTargetJar() throws HopException {

    List<String> systemXmlFiles = Arrays.asList( Const.XML_FILE_HOP_TRANSFORMS, Const.XML_FILE_HOP_EXTENSION_POINTS,
      Const.XML_FILE_HOP_WORKFLOW_ACTIONS, Const.XML_FILE_HOP_DATABASE_TYPES, Const.XML_FILE_HOP_PASSWORD_ENCODER_PLUGINS,
      Const.XML_FILE_HOP_VALUEMETA_PLUGINS, Const.XML_FILE_HOP_PIPELINE_ENGINES, Const.XML_FILE_HOP_TRANSFORMS,
      Const.XML_FILE_HOP_WORKFLOW_ENGINES, Const.XML_FILE_HOP_WORKFLOW_ACTIONS, Const.XML_FILE_HOP_EXTENSION_POINTS,
      Const.XML_FILE_HOP_VFS_PLUGINS, Const.XML_FILE_HOP_SEARCH_ANALYSER_PLUGINS, Const.XML_FILE_HOP_METADATA_PLUGINS );
    fileContentMap = new HashMap<>();

    // The real target file to write to...
    //
    String realTargetJarFile = variables.resolve(targetJarFile);

    try {
      byte[] buffer = new byte[ 1024 ];
      ZipOutputStream zipOutputStream = new ZipOutputStream( new FileOutputStream( realTargetJarFile ) );

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
          if ( systemXmlFiles.contains( entryName ) ) {
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
        // System.out.println( "Entry merged: " + entryName );
        String fileContent = fileContentMap.get( entryName );
        zipOutputStream.putNextEntry( new ZipEntry( entryName ) );
        zipOutputStream.write( fileContent.getBytes( "UTF-8" ) );
        zipOutputStream.closeEntry();
      }

      // The system XML files
      // Core plugins
      DatabasePluginType dbPluginType = DatabasePluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_DATABASE_TYPES, dbPluginType.getMainTag(), dbPluginType.getSubTag(), DatabasePluginType.class, IDatabase.class, null );
      TwoWayPasswordEncoderPluginType pwPluginType = TwoWayPasswordEncoderPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_PASSWORD_ENCODER_PLUGINS, pwPluginType.getMainTag(), pwPluginType.getSubTag(), TwoWayPasswordEncoderPluginType.class, ITwoWayPasswordEncoder.class, null );
      ValueMetaPluginType valuePluginType = ValueMetaPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_VALUEMETA_PLUGINS, valuePluginType.getMainTag(), valuePluginType.getSubTag(), ValueMetaPluginType.class, IValueMeta.class, null );
      // engine plugins
      PipelineEnginePluginType pePluginType = PipelineEnginePluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_PIPELINE_ENGINES, pePluginType.getMainTag(), pePluginType.getSubTag(), PipelineEnginePluginType.class, IPipelineEngine.class, null );
      TransformPluginType transformPluginType = TransformPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_TRANSFORMS, transformPluginType.getMainTag(), transformPluginType.getSubTag(), TransformPluginType.class, ITransformMeta.class, extraTransformPluginClasses );
      WorkflowEnginePluginType wePluginType = WorkflowEnginePluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_WORKFLOW_ENGINES, wePluginType.getMainTag(), wePluginType.getSubTag(), WorkflowEnginePluginType.class, IWorkflowEngine.class, null );
      ActionPluginType actionPluginType = ActionPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_WORKFLOW_ACTIONS, actionPluginType.getMainTag(), actionPluginType.getSubTag(), ActionPluginType.class, IAction.class, null );
      ExtensionPointPluginType xpPluginType = ExtensionPointPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_EXTENSION_POINTS, xpPluginType.getMainTag(), xpPluginType.getSubTag(), ExtensionPointPluginType.class, IExtensionPoint.class, extraXpPluginClasses );
      SearchableAnalyserPluginType saPluginType = SearchableAnalyserPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_SEARCH_ANALYSER_PLUGINS, saPluginType.getMainTag(), saPluginType.getSubTag(), SearchableAnalyserPluginType.class, ISearchableAnalyser.class, null );
      MetadataPluginType mdPluginType = MetadataPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_METADATA_PLUGINS, mdPluginType.getMainTag(), mdPluginType.getSubTag(), MetadataPluginType.class, IHopMetadata.class, null );
      VfsPluginType vfsPluginType = VfsPluginType.getInstance();
      addPluginsXmlFile( zipOutputStream, Const.XML_FILE_HOP_VFS_PLUGINS, vfsPluginType.getMainTag(), vfsPluginType.getSubTag(), VfsPluginType.class, IVfs.class, null );

      zipOutputStream.close();
    } catch ( Exception e ) {
      throw new HopException( "Unable to build far jar file '" + realTargetJarFile + "'", e );
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
      addPluginToXml( xml, pluginTag, plugin, mainPluginClass );
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
      plugins = "hop-beam";
    } else {
      plugins += ",hop-beam";
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
   * Gets extraTransformPluginClasses
   *
   * @return value of extraTransformPluginClasses
   */
  public String getExtraTransformPluginClasses() {
    return extraTransformPluginClasses;
  }

  /**
   * @param extraTransformPluginClasses The extraTransformPluginClasses to set
   */
  public void setExtraTransformPluginClasses( String extraTransformPluginClasses ) {
    this.extraTransformPluginClasses = extraTransformPluginClasses;
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
