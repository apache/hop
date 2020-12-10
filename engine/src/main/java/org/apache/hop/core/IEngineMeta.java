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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.IXml;

import java.util.Date;

public interface IEngineMeta {

  /**
   * Sets the filename.
   *
   * @param filename
   */
  void setFilename( String filename );

  /**
   * Set the name of the engine metadata object
   *
   * @param name
   */
  void setName( String name );

  /**
   * Gets the name.
   *
   * @return name
   */
  String getName();

  /**
   * Builds a name for this. If no name is yet set, create the name from the filename.
   */
  void nameFromFilename();

  /**
   * Clears the changed flag of this.
   */
  void clearChanged();

  /**
   * Gets the XML representation.
   *
   * @return the XML representation of this pipeline
   * @throws HopException if any errors occur during generation of the XML
   * @see IXml#getXml()
   */

  String getXml() throws HopException;

  /**
   * Gets the date the pipeline was created.
   *
   * @return the date the pipeline was created
   */
  Date getCreatedDate();

  /**
   * Sets the date the pipeline was created.
   *
   * @param date The creation date to set
   */
  void setCreatedDate( Date date );

  /**
   * Returns whether or not the this can be saved.
   *
   * @return
   */
  boolean canSave();

  /**
   * Gets the user by whom this was created.
   *
   * @return the user by whom this was created
   */
  String getCreatedUser();

  /**
   * Sets the user by whom this was created.
   *
   * @param createduser The user to set
   */
  void setCreatedUser( String createduser );

  /**
   * Gets the date this was modified.
   *
   * @return the date this was modified
   */
  Date getModifiedDate();

  /**
   * Sets the date this was modified.
   *
   * @param date The modified date to set
   */
  void setModifiedDate( Date date );

  /**
   * Sets the user who last modified this.
   *
   * @param user The user name to set
   */
  void setModifiedUser( String user );

  /**
   * Gets the user who last modified this.
   *
   * @return the user who last modified this
   */
  String getModifiedUser();

  /**
   * Get the filename (if any).
   *
   * @return the filename
   */
  String getFilename();

  /**
   * Sets the internal hop variables on the provided IVariables
   * @param variables the variables to set the internal Hop variables and values on
   */
  void setInternalHopVariables( IVariables variables );

  /**
   * @return true if the name is derived from the filename
   */
  boolean isNameSynchronizedWithFilename();

  /**
   * @param nameSynchronizedWithFilename Set to true if the name is derived from the filename
   */
  void setNameSynchronizedWithFilename( boolean nameSynchronizedWithFilename );

}
