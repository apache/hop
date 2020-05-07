package org.apache.hop.beam.pipeline.main;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.metastore.SerializableMetaStore;
import org.apache.hop.beam.metastore.BeamJobConfig;
import org.apache.hop.beam.pipeline.HopBeamPipelineExecutor;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.PipelineMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainBeam {

  public static final int mainMethod( final String[] args, final String environment ) {

    try {
      System.out.println( "Starting clustered pipeline execution on environment: '"+environment+"'" );

      System.out.println( "Hop Pipeline hpl   / args[0] : " + args[ 0 ] );
      System.out.println( "MetaStore JSON     / args[1] : " + args[ 1 ] );
      System.out.println( "Beam Job Config    / args[2] : " + args[ 2 ] );

      // Read the transformation XML and MetaStore from Hadoop FS
      //
      Configuration hadoopConfiguration = new Configuration();
      String pipelineMetaXml = readFileIntoString( args[ 0 ], hadoopConfiguration, "UTF-8" );
      String metaStoreJson = readFileIntoString( args[ 1 ], hadoopConfiguration, "UTF-8" );

      // Third argument: the beam job config
      //
      String jobConfigName = args[ 2 ];

      // Inflate the metaStore...
      //
      IMetaStore metaStore = new SerializableMetaStore( metaStoreJson );

      System.out.println( ">>>>>> Loading Kettle Beam Job Config '" + jobConfigName + "'" );
      MetaStoreFactory<BeamJobConfig> configFactory = new MetaStoreFactory<>( BeamJobConfig.class, metaStore );
      BeamJobConfig jobConfig = configFactory.loadElement( jobConfigName );

      List<String> stepPluginsList = new ArrayList<>( Arrays.asList( Const.NVL(jobConfig.getStepPluginClasses(), "").split( "," ) ) );
      List<String> xpPluginsList = new ArrayList<>( Arrays.asList( Const.NVL(jobConfig.getXpPluginClasses(), "").split( "," ) ) );

      System.out.println( ">>>>>> Initializing Kettle runtime (" + stepPluginsList.size() + " transform classes, " + xpPluginsList.size() + " XP classes)" );

      BeamHop.init( stepPluginsList, xpPluginsList );

      System.out.println( ">>>>>> Loading transformation metadata" );
      PipelineMeta pipelineMeta = new PipelineMeta( XmlHandler.loadXmlString( pipelineMetaXml, PipelineMeta.XML_TAG ), null );
      pipelineMeta.setMetaStore( metaStore );


      String hadoopConfDir = System.getenv( "HADOOP_CONF_DIR" );
      System.out.println( ">>>>>> HADOOP_CONF_DIR='" + hadoopConfDir + "'" );

      System.out.println( ">>>>>> Building Apache Beam Kettle Pipeline..." );
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin beamInputPlugin = registry.getPlugin( TransformPluginType.class, BeamConst.STRING_BEAM_INPUT_PLUGIN_ID );
      if ( beamInputPlugin != null ) {
        System.out.println( ">>>>>> Found Beam Input transform plugin class loader" );
      } else {
        throw new HopException( "Unable to find Beam Input transform plugin, bailing out!" );
      }
      ClassLoader pluginClassLoader = PluginRegistry.getInstance().getClassLoader( beamInputPlugin );
      if ( pluginClassLoader != null ) {
        System.out.println( ">>>>>> Found Beam Input transform plugin class loader" );
      } else {
        System.out.println( ">>>>>> NOT found Beam Input transform plugin class loader, using system classloader" );
        pluginClassLoader = ClassLoader.getSystemClassLoader();
      }
      HopBeamPipelineExecutor executor = new HopBeamPipelineExecutor( LogChannel.GENERAL, pipelineMeta, jobConfig, metaStore, pluginClassLoader, stepPluginsList, xpPluginsList );

      System.out.println( ">>>>>> Pipeline executing starting..." );
      executor.setLoggingMetrics( true );
      executor.execute( true );
      System.out.println( ">>>>>> Execution finished..." );
      return 0;
    } catch ( Exception e ) {
      System.err.println( "Error running Beam pipeline on '"+environment+"': " + e.getMessage() );
      e.printStackTrace();
      return 1;
    }

  }

  private static String readFileIntoString( String filename, Configuration hadoopConfiguration, String encoding ) throws IOException {
    Path path = new Path( filename );
    FileSystem fileSystem = FileSystem.get( path.toUri(), hadoopConfiguration );
    FSDataInputStream inputStream = fileSystem.open( path );
    String fileContent = IOUtils.toString( inputStream, encoding );
    return fileContent;
  }
}
