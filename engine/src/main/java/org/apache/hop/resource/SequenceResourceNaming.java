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

package org.apache.hop.resource;

import java.util.Hashtable;
import java.util.Map;

/**
 * With this resource naming scheme we try to keep the original filename. However, if there are multiple files with the
 * same name, we add a sequence nr starting at 2.
 * <p>
 * For example :
 * <p>
 * Load orders.hpl Load orders 2.hpl Load orders 3.hpl etc.
 *
 * @author matt
 */
public class SequenceResourceNaming extends SimpleResourceNaming {

  private Map<String, Integer> sequenceMap;

  public SequenceResourceNaming() {
    sequenceMap = new Hashtable<>();
  }

  //
  // End result could look like any of the following:
  //
  // Inputs:
  // Prefix : Marc Sample Pipeline
  // Original Path: D:\japps\hop\samples
  // Extension : .hpl
  //
  // Output Example 1 (no file system prefix, no path used)
  // Marc_Sample_Pipeline_001.hpl
  // Output Example 2 (file system prefix: ${HOP_FILE_BASE}!, no path used)
  // ${HOP_FILE_BASE}!Marc_Sample_Pipeline_003.hpl
  // Output Example 3 (file system prefix: ${HOP_FILE_BASE}!, path is used)
  // ${HOP_FILE_BASE}!japps/hop/samples/Marc_Sample_Pipeline_014.hpl

  protected String getFileNameUniqueIdentifier( String filename, String extension ) {

    String key = filename + extension;
    Integer seq = sequenceMap.get( key );
    if ( seq == null ) {
      seq = new Integer( 2 );
      sequenceMap.put( key, seq );
      return null;
    }

    sequenceMap.put( key, new Integer( seq.intValue() + 1 ) );

    return seq.toString();
  }

}
