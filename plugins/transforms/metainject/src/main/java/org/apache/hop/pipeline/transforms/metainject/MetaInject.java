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

package org.apache.hop.pipeline.transforms.metainject;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Read a simple CSV file Just output Strings found in the file...
 *
 * @author Matt
 * @since 2007-07-05
 */
public class MetaInject extends BaseTransform<MetaInjectMeta, MetaInjectData> implements ITransform<MetaInjectMeta, MetaInjectData> {
  private static final Class<?> PKG = MetaInject.class; // For Translator

  //Added for PDI-17530
  private static final Lock repoSaveLock = new ReentrantLock();

  public MetaInject(
          TransformMeta transformMeta, MetaInjectMeta meta, MetaInjectData data, int copyNr, PipelineMeta pipelineMeta, Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, trans );
  }

  public boolean processRow( ) throws HopException {

    // Read the data from all input transforms and keep it in memory...
    // Skip the transform from which we stream data. Keep that available for runtime action.
    //
    data.rowMap = new HashMap<>();
    for ( String prevTransformName : getPipelineMeta().getPrevTransformNames( getTransformMeta() ) ) {
      // Don't read from the streaming source transform
      //
      if ( !data.streaming || !prevTransformName.equalsIgnoreCase( data.streamingSourceTransformName) ) {
        List<RowMetaAndData> list = new ArrayList<>();
        IRowSet rowSet = findInputRowSet( prevTransformName );
        Object[] row = getRowFrom( rowSet );
        while ( row != null ) {
          RowMetaAndData rd = new RowMetaAndData();
          rd.setRowMeta( rowSet.getRowMeta() );
          rd.setData( row );
          list.add( rd );

          row = getRowFrom( rowSet );
        }
        if ( !list.isEmpty() ) {
          data.rowMap.put( prevTransformName, list );
        }
      }
    }

    List<TransformMeta> transforms = data.pipelineMeta.getTransforms();
    for ( Entry<String, ITransformMeta> en : data.transformInjectionMetasMap.entrySet() ) {
      newInjection( en.getKey(), en.getValue() );
    }
    /*
     * constants injection should be executed after transforms, because if constant should be inserted into target with array
     * in path, constants should be inserted into all arrays items
     */
    for ( Entry<String, ITransformMeta> en : data.transformInjectionMetasMap.entrySet() ) {
      newInjectionConstants( en.getKey(), en.getValue() );
    }
    for ( Entry<String, ITransformMeta> en : data.transformInjectionMetasMap.entrySet() ) {
      en.getValue().searchInfoAndTargetTransforms( transforms );
    }

    for ( String targetTransformName : data.transformInjectionMetasMap.keySet() ) {
      if ( !data.transformInjectionMetasMap.containsKey( targetTransformName ) ) {
        TransformMeta targetTransform = TransformMeta.findTransform( transforms, targetTransformName );
        if ( targetTransform != null ) {
          targetTransform.getTransform().searchInfoAndTargetTransforms( transforms );
        }
      }
    }

    if ( !meta.isNoExecution() ) {
      // Now we can execute this modified transformation metadata.
      //
      final Pipeline injectPipeline = createInjectPipeline();
      injectPipeline.setParentPipeline( getPipeline() );
      injectPipeline.setMetadataProvider( getMetadataProvider() );
      if ( getPipeline().getParentWorkflow() != null ) {
        injectPipeline.setParentWorkflow( getPipeline().getParentWorkflow() ); // See PDI-13224
      }

      // Copy all variables over...
      //
      injectPipeline.copyFrom( this );

      // Copy parameter definitions with empty values.
      // Then set those parameters to the values if have any.

      injectPipeline.copyParametersFromDefinitions( data.pipelineMeta );
      for (String variableName : injectPipeline.getVariableNames()) {
        String variableValue = getVariable( variableName );
        if ( StringUtils.isNotEmpty(variableValue)) {
          injectPipeline.setParameterValue( variableName, variableValue );
        }
      }

      getPipeline().addExecutionStoppedListener(e -> injectPipeline.stopAll());

      injectPipeline.setLogLevel( getLogLevel() );

      // Parameters get activated below so we need to make sure they have values
      //
      injectPipeline.prepareExecution( );

      // See if we need to stream some data over...
      //
      RowProducer rowProducer = null;
      if ( data.streaming ) {
        rowProducer = injectPipeline.addRowProducer( data.streamingTargetTransformName, 0 );
      }

      // Finally, add the mapping transformation to the active sub-transformations
      // map in the parent transformation
      //
      getPipeline().addActiveSubPipeline( getTransformName(), injectPipeline );

      if ( !Utils.isEmpty( meta.getSourceTransformName() ) ) {
        ITransform transformInterface = injectPipeline.getTransformInterface( meta.getSourceTransformName(), 0 );
        if ( transformInterface == null ) {
          throw new HopException( "Unable to find transform '" + meta.getSourceTransformName() + "' to read from." );
        }
        transformInterface.addRowListener( new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row ) throws HopTransformException {
            // Just pass along the data as output of this transform...
            //
            MetaInject.this.putRow( rowMeta, row );
          }
        } );
      }

      injectPipeline.startThreads();

      if ( data.streaming ) {
        // Deplete all the rows from the parent transformation into the modified transformation
        //
        IRowSet rowSet = findInputRowSet( data.streamingSourceTransformName);
        if ( rowSet == null ) {
          throw new HopException( "Unable to find transform '" + data.streamingSourceTransformName + "' to stream data from" );
        }
        Object[] row = getRowFrom( rowSet );
        while ( row != null && !isStopped() ) {
          rowProducer.putRow( rowSet.getRowMeta(), row );
          row = getRowFrom( rowSet );
        }
        rowProducer.finished();
      }

      // Wait until the child transformation finished processing...
      //
      while ( !injectPipeline.isFinished() && !injectPipeline.isStopped() && !isStopped() ) {
        copyResult( injectPipeline );

        // Wait a little bit.
        try {
          Thread.sleep( 50 );
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
      copyResult( injectPipeline );
      waitUntilFinished( injectPipeline );
    }

    // let the transformation complete it's execution to allow for any customizations to MDI to happen in the init methods of transforms
    if ( log.isDetailed() ) {
      logDetailed( "XML of transformation after injection: " + data.pipelineMeta.getXml() );
    }
    String targetFile = resolve( meta.getTargetFile() );
    if ( !Utils.isEmpty( targetFile ) ) {
      writeInjectedHpl( targetFile );
    }

    // All done!

    setOutputDone();

    return false;
  }

  void waitUntilFinished( Pipeline injectTrans ) {
    injectTrans.waitUntilFinished();
  }

  Pipeline createInjectPipeline() {
    return new LocalPipelineEngine( data.pipelineMeta, this, this );
  }

  private void writeInjectedHpl(String targetFilPath ) throws HopException {

    writeInjectedHplToFs( targetFilPath );
  }

  /**
   * Writes the generated meta injection transformation to the file system.
   * @param targetFilePath the filesystem path to which to save the generated injection hpl
   * @throws HopException
   */
  private void writeInjectedHplToFs(String targetFilePath ) throws HopException {

    OutputStream os = null;
    try {
      os = HopVfs.getOutputStream( targetFilePath, false );
      os.write( XmlHandler.getXmlHeader().getBytes( Const.XML_ENCODING ) );
      os.write( data.pipelineMeta.getXml().getBytes( Const.XML_ENCODING ) );
    } catch ( IOException e ) {
      throw new HopException( "Unable to write target file (hpl after injection) to file '"
        + targetFilePath + "'", e );
    } finally {
      if ( os != null ) {
        try {
          os.close();
        } catch ( Exception e ) {
          throw new HopException( e );
        }
      }
    }
  }

  /**
   * Inject values from transforms.
   */
  private void newInjection( String targetTransform, ITransformMeta targetTransformMeta ) throws HopException {
    if ( log.isDetailed() ) {
      logDetailed( "Handing transform '" + targetTransform + "' injection!" );
    }
    BeanInjectionInfo injectionInfo = new BeanInjectionInfo( targetTransformMeta.getClass() );
    BeanInjector injector = new BeanInjector( injectionInfo );

    // Collect all the metadata for this target transform...
    //
    Map<TargetTransformAttribute, SourceTransformField> targetMap = meta.getTargetSourceMapping();
    boolean wasInjection = false;
    for ( TargetTransformAttribute target : targetMap.keySet() ) {
      SourceTransformField source = targetMap.get( target );

      if ( target.getTransformName().equalsIgnoreCase( targetTransform ) ) {
        // This is the transform to collect data for...
        // We also know which transform to read the data from. (source)
        //
        if ( source.getTransformName() != null ) {
          // from specified transform
          List<RowMetaAndData> rows = data.rowMap.get( source.getTransformName() );
          if ( rows != null && !rows.isEmpty() ) {
            // Which metadata key is this referencing? Find the attribute key in the metadata entries...
            //
            if ( injector.hasProperty( targetTransformMeta, target.getAttributeKey() ) ) {
              // target transform has specified key
              boolean skip = false;
              for ( RowMetaAndData r : rows ) {
                if ( r.getRowMeta().indexOfValue( source.getField() ) < 0 ) {
                  logError( BaseMessages.getString( PKG, "MetaInject.SourceFieldIsNotDefined.Message", source
                    .getField(), getPipelineMeta().getName() ) );
                  // source transform doesn't contain specified field
                  skip = true;
                }
              }
              if ( !skip ) {
                // specified field exist - need to inject
                injector.setProperty( targetTransformMeta, target.getAttributeKey(), rows, source.getField() );
                wasInjection = true;
              }
            } else {
              // target transform doesn't have specified key - just report but don't fail like in 6.0 (BACKLOG-6753)
              logError( BaseMessages.getString( PKG, "MetaInject.TargetKeyIsNotDefined.Message", target
                .getAttributeKey(), getPipelineMeta().getName() ) );
            }
          }
        }
      }
    }
    if ( wasInjection ) {
      injector.runPostInjectionProcessing( targetTransformMeta );
    }
  }

  /**
   * Inject constant values.
   */
  private void newInjectionConstants( String targetTransform, ITransformMeta targetTransformMeta ) throws HopException {
    if ( log.isDetailed() ) {
      logDetailed( "Handing transform '" + targetTransform + "' constants injection!" );
    }
    BeanInjectionInfo injectionInfo = new BeanInjectionInfo( targetTransformMeta.getClass() );
    BeanInjector injector = new BeanInjector( injectionInfo );

    // Collect all the metadata for this target transform...
    //
    Map<TargetTransformAttribute, SourceTransformField> targetMap = meta.getTargetSourceMapping();
    for ( TargetTransformAttribute target : targetMap.keySet() ) {
      SourceTransformField source = targetMap.get( target );

      if ( target.getTransformName().equalsIgnoreCase( targetTransform ) ) {
        // This is the transform to collect data for...
        // We also know which transform to read the data from. (source)
        //
        if ( source.getTransformName() == null ) {
          // inject constant
          if ( injector.hasProperty( targetTransformMeta, target.getAttributeKey() ) ) {
            // target transform has specified key
            injector.setProperty( targetTransformMeta, target.getAttributeKey(), null, source.getField() );
          } else {
            // target transform doesn't have specified key - just report but don't fail like in 6.0 (BACKLOG-6753)
            logError( BaseMessages.getString( PKG, "MetaInject.TargetKeyIsNotDefined.Message", target.getAttributeKey(),
              getPipelineMeta().getName() ) );
          }
        }
      }
    }
  }

  private void copyResult( Pipeline trans ) {
    Result result = trans.getResult();
    setLinesInput( result.getNrLinesInput() );
    setLinesOutput( result.getNrLinesOutput() );
    setLinesRead( result.getNrLinesRead() );
    setLinesWritten( result.getNrLinesWritten() );
    setLinesUpdated( result.getNrLinesUpdated() );
    setLinesRejected( result.getNrLinesRejected() );
    setErrors( result.getNrErrors() );
  }

  public boolean init( ) {

    if ( super.init( ) ) {
      try {
        meta.actualizeMetaInjectMapping();
        data.pipelineMeta = loadPipelineMeta();
        checkSoureTransformsAvailability();
        checkTargetTransformsAvailability();
        // Get a mapping between the transform name and the injection...
        //
        // Get new injection info
        data.transformInjectionMetasMap = new HashMap<>();
        for ( TransformMeta transformMeta : data.pipelineMeta.getUsedTransforms() ) {
          ITransformMeta meta = transformMeta.getTransform();
          if ( BeanInjectionInfo.isInjectionSupported( meta.getClass() ) ) {
            data.transformInjectionMetasMap.put( transformMeta.getName(), meta );
          }
        }

        // See if we need to stream data from a specific transform into the template
        //
        if ( meta.getStreamSourceTransform() != null && !Utils.isEmpty( meta.getStreamTargetTransformName() ) ) {
          data.streaming = true;
          data.streamingSourceTransformName = meta.getStreamSourceTransform().getName();
          data.streamingTargetTransformName = meta.getStreamTargetTransformName();
        }

        return true;
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "MetaInject.BadEncoding.Message" ), e );
        return false;
      }
    }

    return false;
  }

  private void checkTargetTransformsAvailability() {
    Set<String> existedTransformNames = convertToUpperCaseSet( data.pipelineMeta.getTransformNames() );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = meta.getTargetSourceMapping();
    Set<TargetTransformAttribute> unavailableTargetTransforms = getUnavailableTargetTransforms( targetMap, data.pipelineMeta);
    Set<String> alreadyMarkedTransforms = new HashSet<>();
    for ( TargetTransformAttribute currentTarget : unavailableTargetTransforms ) {
      if ( alreadyMarkedTransforms.contains( currentTarget.getTransformName() ) ) {
        continue;
      }
      alreadyMarkedTransforms.add( currentTarget.getTransformName() );
      if ( existedTransformNames.contains( currentTarget.getTransformName().toUpperCase() ) ) {
        logError( BaseMessages.getString( PKG, "MetaInject.TargetTransformIsNotUsed.Message", currentTarget.getTransformName(),
          data.pipelineMeta.getName() ) );
      } else {
        logError( BaseMessages.getString( PKG, "MetaInject.TargetTransformIsNotDefined.Message", currentTarget.getTransformName(),
          data.pipelineMeta.getName() ) );
      }
    }
    // alreadyMarked contains wrong transforms. Hop-Gui can report error if it will not fail transformation [BACKLOG-6753]
  }

  public static void removeUnavailableTransformsFromMapping(Map<TargetTransformAttribute, SourceTransformField> targetMap,
                                                            Set<SourceTransformField> unavailableSourceTransforms, Set<TargetTransformAttribute> unavailableTargetTransforms ) {
    Iterator<Entry<TargetTransformAttribute, SourceTransformField>> targetMapIterator = targetMap.entrySet().iterator();
    while ( targetMapIterator.hasNext() ) {
      Entry<TargetTransformAttribute, SourceTransformField> entry = targetMapIterator.next();
      SourceTransformField currentSourceTransformField = entry.getValue();
      TargetTransformAttribute currentTargetTransformAttribute = entry.getKey();
      if ( unavailableSourceTransforms.contains(currentSourceTransformField) || unavailableTargetTransforms.contains(
              currentTargetTransformAttribute) ) {
        targetMapIterator.remove();
      }
    }
  }

  public static Set<TargetTransformAttribute> getUnavailableTargetTransforms(Map<TargetTransformAttribute, SourceTransformField> targetMap,
                                                                             PipelineMeta injectedPipelineMeta ) {
    Set<String> usedTransformNames = getUsedTransformsForReferencendTransformation( injectedPipelineMeta );
    Set<TargetTransformAttribute> unavailableTargetTransforms = new HashSet<>();
    for ( TargetTransformAttribute currentTarget : targetMap.keySet() ) {
      if ( !usedTransformNames.contains( currentTarget.getTransformName().toUpperCase() ) ) {
        unavailableTargetTransforms.add( currentTarget );
      }
    }
    return Collections.unmodifiableSet( unavailableTargetTransforms );
  }

  public static Set<TargetTransformAttribute> getUnavailableTargetKeys(Map<TargetTransformAttribute, SourceTransformField> targetMap,
                                                                       PipelineMeta injectedPipelineMeta, Set<TargetTransformAttribute> unavailableTargetTransforms ) {
    Set<TargetTransformAttribute> missingKeys = new HashSet<>();
    Map<String, BeanInjectionInfo> beanInfos = getUsedTransformBeanInfos( injectedPipelineMeta );
    for ( TargetTransformAttribute key : targetMap.keySet() ) {
      if ( !unavailableTargetTransforms.contains( key ) ) {
        BeanInjectionInfo info = beanInfos.get( key.getTransformName().toUpperCase() );
        if ( info != null && !info.getProperties().containsKey( key.getAttributeKey() ) ) {
          missingKeys.add( key );
        }
      }
    }
    return missingKeys;
  }

  private static Map<String, BeanInjectionInfo> getUsedTransformBeanInfos(PipelineMeta pipelineMeta ) {
    Map<String, BeanInjectionInfo> res = new HashMap<>();
    for ( TransformMeta transformMeta : pipelineMeta.getUsedTransforms() ) {
      Class<? extends ITransformMeta> transformMetaClass = transformMeta.getTransform().getClass();
      if ( BeanInjectionInfo.isInjectionSupported( transformMetaClass ) ) {
        res.put( transformMeta.getName().toUpperCase(), new BeanInjectionInfo( transformMetaClass ) );
      }
    }
    return res;
  }

  private static Set<String> getUsedTransformsForReferencendTransformation(PipelineMeta pipelineMeta ) {
    Set<String> usedTransformNames = new HashSet<>();
    for ( TransformMeta currentTransform : pipelineMeta.getUsedTransforms() ) {
      usedTransformNames.add( currentTransform.getName().toUpperCase() );
    }
    return usedTransformNames;
  }

  public static Set<SourceTransformField> getUnavailableSourceTransforms(Map<TargetTransformAttribute, SourceTransformField> targetMap,
                                                                         PipelineMeta sourcePipelineMeta, TransformMeta transformMeta ) {
    String[] transformNamesArray = sourcePipelineMeta.getPrevTransformNames( transformMeta );
    Set<String> existedTransformNames = convertToUpperCaseSet( transformNamesArray );
    Set<SourceTransformField> unavailableSourceTransforms = new HashSet<>();
    for ( SourceTransformField currentSource : targetMap.values() ) {
      if ( currentSource.getTransformName() != null ) {
        if ( !existedTransformNames.contains( currentSource.getTransformName().toUpperCase() ) ) {
          unavailableSourceTransforms.add( currentSource );
        }
      }
    }
    return Collections.unmodifiableSet( unavailableSourceTransforms );
  }

  private void checkSoureTransformsAvailability() {
    Map<TargetTransformAttribute, SourceTransformField> targetMap = meta.getTargetSourceMapping();
    Set<SourceTransformField> unavailableSourceTransforms =
      getUnavailableSourceTransforms( targetMap, getPipelineMeta(), getTransformMeta() );
    Set<String> alreadyMarkedTransforms = new HashSet<>();
    for ( SourceTransformField currentSource : unavailableSourceTransforms ) {
      if ( alreadyMarkedTransforms.contains( currentSource.getTransformName() ) ) {
        continue;
      }
      alreadyMarkedTransforms.add( currentSource.getTransformName() );
      logError( BaseMessages.getString( PKG, "MetaInject.SourceTransformIsNotAvailable.Message", currentSource.getTransformName(),
        getPipelineMeta().getName() ) );
    }
    // alreadyMarked contains wrong transforms. Spoon can report error if it will not fail transformation [BACKLOG-6753]
  }

  /**
   * package-local visibility for testing purposes
   */
  static Set<String> convertToUpperCaseSet( String[] array ) {
    if ( array == null ) {
      return Collections.emptySet();
    }
    Set<String> strings = new HashSet<>();
    for ( String currentString : array ) {
      strings.add( currentString.toUpperCase() );
    }
    return strings;
  }

  /**
   * package-local visibility for testing purposes
   */
  PipelineMeta loadPipelineMeta() throws HopException {
    return MetaInjectMeta.loadPipelineMeta( meta, getPipeline().getMetadataProvider(), this );
  }

}
