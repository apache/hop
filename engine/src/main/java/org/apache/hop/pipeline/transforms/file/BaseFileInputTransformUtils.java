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

package org.apache.hop.pipeline.transforms.file;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;

import java.util.List;

/**
 * Utils for file-based input transforms.
 *
 * @author Alexander Buloichik
 */
public class BaseFileInputTransformUtils {

  public static void handleMissingFiles( FileInputList files, ILogChannel log, boolean isErrorIgnored,
                                         IFileErrorHandler errorHandler ) throws HopException {
    List<FileObject> nonExistantFiles = files.getNonExistantFiles();

    if ( !nonExistantFiles.isEmpty() ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      if ( log.isBasic() ) {
        log.logBasic( "Required files", "WARNING: Missing " + message );
      }
      if ( isErrorIgnored ) {
        for ( FileObject fileObject : nonExistantFiles ) {
          errorHandler.handleNonExistantFile( fileObject );
        }
      } else {
        throw new HopException( "Following required files are missing: " + message );
      }
    }

    List<FileObject> nonAccessibleFiles = files.getNonAccessibleFiles();
    if ( !nonAccessibleFiles.isEmpty() ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      if ( log.isBasic() ) {
        log.logBasic( "Required files", "WARNING: Not accessible " + message );
      }
      if ( isErrorIgnored ) {
        for ( FileObject fileObject : nonAccessibleFiles ) {
          errorHandler.handleNonAccessibleFile( fileObject );
        }
      } else {
        throw new HopException( "Following required files are not accessible: " + message );
      }
    }
  }

  /**
   * Adds <code>String</code> value meta with given name if not present and returns index
   *
   * @param rowMeta
   * @param fieldName
   * @return Index in row meta of value meta with <code>fieldName</code>
   */
  public static int addValueMeta( String transformName, IRowMeta rowMeta, String fieldName ) {
    IValueMeta valueMeta = new ValueMetaString( fieldName );
    valueMeta.setOrigin( transformName );
    // add if doesn't exist
    int index = -1;
    if ( !rowMeta.exists( valueMeta ) ) {
      index = rowMeta.size();
      rowMeta.addValueMeta( valueMeta );
    } else {
      index = rowMeta.indexOfValue( fieldName );
    }
    return index;
  }
}
