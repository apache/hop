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


import com.google.common.base.Preconditions;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceReference;

import java.util.List;

/**
 * Base meta for file-based input transforms.
 *
 * @author Alexander Buloichik
 */
public abstract class BaseFileInputMeta<A extends BaseFileInputAdditionalField, I extends BaseFileInputFiles, F extends BaseFileField, Main extends ITransform, Data extends ITransformData>
  extends BaseTransformMeta implements
  ITransformMeta<Main, Data> {
  private static final Class<?> PKG = BaseFileInputMeta.class; // For Translator

  public static final String[] RequiredFilesCode = new String[] { "N", "Y" };

  public static final String NO = "N";

  public static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
    new String[] { BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG,
        "System.Combo.Yes" ) };

  @InjectionDeep
  public I inputFiles;

  /**
   * The fields to import...
   */
  @InjectionDeep
  public F[] inputFields;

  /**
   * @return the input fields.
   */
  public F[] getInputFields() {
    return inputFields;
  }

  @InjectionDeep
  public BaseFileErrorHandling errorHandling = new BaseFileErrorHandling();
  @InjectionDeep
  public A additionalOutputFields;

  public Object clone() {
    BaseFileInputMeta<BaseFileInputAdditionalField, BaseFileInputFiles, BaseFileField, BaseFileInputTransform, BaseFileInputTransformData> retval =
      (BaseFileInputMeta<BaseFileInputAdditionalField, BaseFileInputFiles, BaseFileField, BaseFileInputTransform, BaseFileInputTransformData>) super.clone();

    retval.inputFiles = (BaseFileInputFiles) inputFiles.clone();
    retval.errorHandling = (BaseFileErrorHandling) errorHandling.clone();
    retval.additionalOutputFields = (BaseFileInputAdditionalField) additionalOutputFields.clone();

    return retval;
  }

  /**
   * @param fileRequiredin The fileRequired to set.
   */
  public void inputFiles_fileRequired( String[] fileRequiredin ) {
    for ( int i = 0; i < fileRequiredin.length; i++ ) {
      inputFiles.fileRequired[ i ] = getRequiredFilesCode( fileRequiredin[ i ] );
    }
  }

  public String[] inputFiles_includeSubFolders() {
    return inputFiles.includeSubFolders;
  }

  public void inputFiles_includeSubFolders( String[] includeSubFoldersin ) {
    for ( int i = 0; i < includeSubFoldersin.length; i++ ) {
      inputFiles.includeSubFolders[ i ] = getRequiredFilesCode( includeSubFoldersin[ i ] );
    }
  }

  public static String getRequiredFilesCode( String tt ) {
    if ( tt == null ) {
      return RequiredFilesCode[ 0 ];
    }
    if ( tt.equals( RequiredFilesDesc[ 1 ] ) ) {
      return RequiredFilesCode[ 1 ];
    } else {
      return RequiredFilesCode[ 0 ];
    }
  }

  public FileInputList getFileInputList( IVariables variables ) {
    inputFiles.normalizeAllocation( inputFiles.fileName.length );
    return FileInputList.createFileList( variables, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
      inputFiles.fileRequired, inputFiles.includeSubFolderBoolean() );
  }

  @Override
  public List<ResourceReference> getResourceDependencies( IVariables variables, TransformMeta transformMeta ) {
    return inputFiles.getResourceDependencies( variables, transformMeta );
  }

  public abstract String getEncoding();

  public boolean isAcceptingFilenames() {
    Preconditions.checkNotNull( inputFiles );
    return inputFiles.acceptingFilenames;
  }

  public String getAcceptingTransformName() {
    return inputFiles == null ? null : inputFiles.acceptingTransformName;
  }

  public String getAcceptingField() {
    return inputFiles == null ? null : inputFiles.acceptingField;
  }

  public String[] getFilePaths( IVariables variables, final boolean showSamples ) {
    final TransformMeta parentTransformMeta = getParentTransformMeta();
    if ( parentTransformMeta != null ) {
      final PipelineMeta parentPipelineMeta = parentTransformMeta.getParentPipelineMeta();
      if ( parentPipelineMeta != null ) {
        final FileInputList inputList = getFileInputList( variables );
        if ( inputList != null ) {
          return inputList.getFileStrings();
        }
      }
    }
    return new String[] {};
  }
}
