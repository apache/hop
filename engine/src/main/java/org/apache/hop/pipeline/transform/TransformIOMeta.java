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

package org.apache.hop.pipeline.transform;

import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.List;

public class TransformIOMeta implements ITransformIOMeta, Cloneable {
  private boolean inputAcceptor;
  private boolean outputProducer;
  private boolean inputOptional;
  private boolean outputDynamic;
  private boolean inputDynamic;

  private List<IStream> streams;
  private boolean sortedDataRequired;

  private String generalInfoDescription;
  private String generalTargetDescription;

  /**
   * @param inputAcceptor
   * @param outputProducer
   */
  public TransformIOMeta( boolean inputAcceptor, boolean outputProducer, boolean inputOptional,
                          boolean sortedDataRequired, boolean inputDynamic, boolean outputDynamic ) {
    this.inputAcceptor = inputAcceptor;
    this.outputProducer = outputProducer;
    this.inputOptional = inputOptional;
    this.sortedDataRequired = sortedDataRequired;
    this.streams = java.util.Collections.synchronizedList( new ArrayList<>() );
    this.inputDynamic = inputDynamic;
    this.outputDynamic = outputDynamic;
  }

  @Override
  protected TransformIOMeta clone() throws CloneNotSupportedException {
    TransformIOMeta ioMeta = (TransformIOMeta) super.clone();

    List<IStream> cloneStreams = new ArrayList<>( streams );
    ioMeta.streams = java.util.Collections.synchronizedList( cloneStreams );
    return ioMeta;
  }

  /**
   * @return the inputAcceptor
   */
  public boolean isInputAcceptor() {
    return inputAcceptor;
  }

  /**
   * @param inputAcceptor the inputAcceptor to set
   */
  public void setInputAcceptor( boolean inputAcceptor ) {
    this.inputAcceptor = inputAcceptor;
  }

  /**
   * @return the outputProducer
   */
  public boolean isOutputProducer() {
    return outputProducer;
  }

  /**
   * @param outputProducer the outputProducer to set
   */
  public void setOutputProducer( boolean outputProducer ) {
    this.outputProducer = outputProducer;
  }

  /**
   * @return the inputOptional
   */
  public boolean isInputOptional() {
    return inputOptional;
  }

  /**
   * @param inputOptional the inputOptional to set
   */
  public void setInputOptional( boolean inputOptional ) {
    this.inputOptional = inputOptional;
  }

  /**
   * @return the info streams of this transform. Important: Modifying this list does not have any effect on the Transforms IO
   * metadata.
   */
  public List<IStream> getInfoStreams() {
    List<IStream> list = new ArrayList<>();
    synchronized ( streams ) {
      for ( IStream stream : streams ) {
        if ( stream.getStreamType().equals( IStream.StreamType.INFO ) ) {
          list.add( stream );
        }
      }
    }
    return list;
  }

  /**
   * @return the target streams of this transform. Important: Modifying this list does not have any effect on the Transforms IO
   * metadata.
   */
  public List<IStream> getTargetStreams() {
    List<IStream> list = new ArrayList<>();
    synchronized ( streams ) {
      for ( IStream stream : streams ) {
        if ( stream.getStreamType().equals( IStream.StreamType.TARGET ) ) {
          list.add( stream );
        }
      }
    }
    return list;
  }

  /**
   * @return the sortedDataRequired
   */
  public boolean isSortedDataRequired() {
    return sortedDataRequired;
  }

  /**
   * @param sortedDataRequired the sortedDataRequired to set
   */
  public void setSortedDataRequired( boolean sortedDataRequired ) {
    this.sortedDataRequired = sortedDataRequired;
  }

  public void addStream( IStream stream ) {
    streams.add( stream );
  }

  public String[] getInfoTransformNames() {
    List<IStream> infoStreams = getInfoStreams();
    String[] names = new String[ infoStreams.size() ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = infoStreams.get( i ).getTransformName();
    }
    return names;
  }

  public String[] getTargetTransformNames() {
    List<IStream> targetStreams = getTargetStreams();
    String[] names = new String[ targetStreams.size() ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = targetStreams.get( i ).getTransformName();
    }
    return names;
  }

  /**
   * Replace the info transforms with the supplied source transforms.
   *
   * @param infoTransforms
   */
  public void setInfoTransforms( TransformMeta[] infoTransforms ) {
    // First get the info transforms...
    //
    List<IStream> list = new ArrayList<>();
    synchronized ( streams ) {
      for ( IStream stream : streams ) {
        if ( stream.getStreamType().equals( IStream.StreamType.INFO ) ) {
          list.add( stream );
        }
      }
    }
    for ( int i = 0; i < infoTransforms.length; i++ ) {
      if ( i >= list.size() ) {
        throw new RuntimeException( "We expect all possible info streams to be pre-populated!" );
      }
      streams.get( i ).setTransformMeta( infoTransforms[ i ] );
    }
  }

  /**
   * @return the generalInfoDescription
   */
  public String getGeneralInfoDescription() {
    return generalInfoDescription;
  }

  /**
   * @param generalInfoDescription the generalInfoDescription to set
   */
  public void setGeneralInfoDescription( String generalInfoDescription ) {
    this.generalInfoDescription = generalInfoDescription;
  }

  /**
   * @return the generalTargetDescription
   */
  public String getGeneralTargetDescription() {
    return generalTargetDescription;
  }

  /**
   * @param generalTargetDescription the generalTargetDescription to set
   */
  public void setGeneralTargetDescription( String generalTargetDescription ) {
    this.generalTargetDescription = generalTargetDescription;
  }

  public void clearStreams() {
    streams.clear();
  }

  /**
   * @return the outputDynamic
   */
  public boolean isOutputDynamic() {
    return outputDynamic;
  }

  /**
   * @param outputDynamic the outputDynamic to set
   */
  public void setOutputDynamic( boolean outputDynamic ) {
    this.outputDynamic = outputDynamic;
  }

  /**
   * @return the inputDynamic
   */
  public boolean isInputDynamic() {
    return inputDynamic;
  }

  /**
   * @param inputDynamic the inputDynamic to set
   */
  public void setInputDynamic( boolean inputDynamic ) {
    this.inputDynamic = inputDynamic;
  }

  public IStream findTargetStream( TransformMeta targetTransform ) {
    for ( IStream stream : getTargetStreams() ) {
      if ( targetTransform.equals( stream.getTransformMeta() ) ) {
        return stream;
      }
    }
    return null;
  }

  public IStream findInfoStream( TransformMeta infoTransform ) {
    for ( IStream stream : getInfoStreams() ) {
      if ( infoTransform.equals( stream.getTransformMeta() ) ) {
        return stream;
      }
    }
    return null;
  }
}
