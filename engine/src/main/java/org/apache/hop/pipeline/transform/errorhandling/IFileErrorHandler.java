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

package org.apache.hop.pipeline.transform.errorhandling;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;

/**
 * @author Johnny Vanhentenyk
 */
public interface IFileErrorHandler {

  /**
   * Tells the handler which file is being processed.
   *
   * @param file
   * @throws HopException
   */
  void handleFile( FileObject file ) throws HopException;

  /**
   * This method handles an error when processing the line with corresponding lineNr.
   *
   * @param lineNr
   * @param filePart allows us to split error according to a filePart
   * @throws HopException
   */
  void handleLineError( long lineNr, String filePart ) throws HopException;

  /**
   * This method closes the handler;
   */
  void close() throws HopException;

  /**
   * This method handles a file that is required, but does not exist.
   *
   * @param file
   * @throws HopException
   */
  void handleNonExistantFile( FileObject file ) throws HopException;

  /**
   * This method handles a file that is required, but is not accessible.
   *
   * @param file
   * @throws HopException
   */
  void handleNonAccessibleFile( FileObject file ) throws HopException;
}
