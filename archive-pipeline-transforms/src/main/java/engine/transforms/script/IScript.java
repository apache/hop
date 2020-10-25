/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.RowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransform;

import java.util.List;

/**
 * Interface to make the real ScriptValueMod and ScriptValueModDummy similar.
 *
 * @author Sven Boden
 */
public interface IScript extends ITransform {
  boolean.processRow() throws HopException;

  void addRowListener( RowListener rowListener );

  void.dispose();

  long getErrors();

  List<RowSet> getInputRowSets();

  long getLinesInput();

  long getLinesOutput();

  long getLinesRead();

  long getLinesUpdated();

  long getLinesWritten();

  long getLinesRejected();

  List<RowSet> getOutputRowSets();

  String getPartitionID();

  Object[] getRow() throws HopException;

  List<RowListener> getRowListeners();

  String getTransformPluginId();

  String getTransformName();

  boolean.init();

  boolean isAlive();

  boolean isPartitioned();

  boolean isStopped();

  void markStart();

  void markStop();

  void putRow( IRowMeta rowMeta, Object[] row ) throws HopException;

  void removeRowListener( RowListener rowListener );

  void run();

  void setErrors( long errors );

  void setOutputDone();

  void setPartitionID( String partitionID );

  void start();

  void stopAll();

  void stopRunning( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException;

  void cleanup();

  void pauseRunning();

  void resumeRunning();
}
