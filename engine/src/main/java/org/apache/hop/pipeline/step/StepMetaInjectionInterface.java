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

package org.apache.hop.pipeline.step;

import org.apache.hop.core.exception.HopException;

import java.util.List;

/**
 * This interface allows an external program to inject metadata using a standard flat set of metadata attributes.
 *
 * @author matt
 * @deprecated Use annotation-based injection instead
 */
@Deprecated
public interface StepMetaInjectionInterface {
  /**
   * @return A list of step injection metadata entries. In case the data type of the entry is NONE (0) you will get at
   * least one entry in the details section. You can use this list to see a description of all the metadata.
   */
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws HopException;

  /**
   * Inject the metadata specified into the step metadata
   *
   * @param metadata the metadata to inject.
   * @throws HopException
   */
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> metadata ) throws HopException;

  /**
   * Extracts the step metadata in a standard fashion.  Enables unit testing more easily by extracting the step
   * metadata and then doing the re-injection into an empty step.
   *
   * @return A list of step injection metadata entries in a standard fashion.  Includes both the entry and the value
   * for the entry.
   * @throws HopException
   */
  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws HopException;
}
