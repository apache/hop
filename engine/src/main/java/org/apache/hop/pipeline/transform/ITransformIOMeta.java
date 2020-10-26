/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transform;

import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.List;

public interface ITransformIOMeta {

  boolean isInputAcceptor();

  boolean isOutputProducer();

  boolean isInputOptional();

  boolean isSortedDataRequired();

  List<IStream> getInfoStreams();

  List<IStream> getTargetStreams();

  String[] getInfoTransformNames();

  String[] getTargetTransformNames();

  /**
   * Replace the info transforms with the supplied source transforms.
   *
   * @param infoTransforms
   */
  void setInfoTransforms( TransformMeta[] infoTransforms );

  /**
   * Add a stream to the transforms I/O interface
   *
   * @param stream The stream to add
   */
  void addStream( IStream stream );

  /**
   * Set the general info stream description
   *
   * @param string the info streams description
   */
  void setGeneralInfoDescription( String string );

  /**
   * Set the general target stream description
   *
   * @param string the target streams description
   */
  void setGeneralTargetDescription( String string );

  /**
   * @return the generalTargetDescription
   */
  String getGeneralTargetDescription();

  /**
   * @return the generalInfoDescription
   */
  String getGeneralInfoDescription();

  /**
   * @return true if the output targets of this transform are dynamic (variable)
   */
  boolean isOutputDynamic();

  /**
   * @param outputDynamic set to true if the output targets of this transform are dynamic (variable)
   */
  void setOutputDynamic( boolean outputDynamic );

  /**
   * @return true if the input info sources of this transform are dynamic (variable)
   */
  boolean isInputDynamic();

  /**
   * @param inputDynamic set to true if the input info sources of this transform are dynamic (variable)
   */
  void setInputDynamic( boolean inputDynamic );

  IStream findTargetStream( TransformMeta targetTransform );

  IStream findInfoStream( TransformMeta infoTransform );
}
