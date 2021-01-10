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

package org.apache.hop.base;

import java.util.Arrays;

public class CommandExecutorCodes {

  public enum Pan {

    SUCCESS( 0, "The pipeline ran without a problem" ),
    ERRORS_DURING_PROCESSING( 1, "Errors occurred during processing" ),
    UNEXPECTED_ERROR( 2, "An unexpected error occurred during loading / running of the pipeline" ),
    UNABLE_TO_PREP_INIT_PIPELINE( 3, "Unable to prepare and initialize this pipeline" ),
    HOP_VERSION_PRINT( 6, "Hop Version printing" ),
    COULD_NOT_LOAD_PIPELINE( 7, "The pipeline couldn't be de-serialized" ),
    ERROR_LOADING_TRANSFORMS_PLUGINS( 8, "Error loading transforms or plugins (error in loading one of the plugins mostly)" ),
    CMD_LINE_PRINT( 9, "Command line usage printing" );

    private int code;
    private String description;

    Pan( int code, String description ) {
      setCode( code );
      setDescription( description );
    }

    public int getCode() {
      return code;
    }

    public void setCode( int code ) {
      this.code = code;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription( String description ) {
      this.description = description;
    }

    public static Pan getByCode( final int code ) {
      return Arrays.asList( Pan.values() ).stream()
        .filter( pan -> pan.getCode() == code )
        .findAny().orElse( null );
    }

    public static boolean isFailedExecution( final int code ) {
      return Pan.UNEXPECTED_ERROR.getCode() == code
        || Pan.UNABLE_TO_PREP_INIT_PIPELINE.getCode() == code
        || Pan.COULD_NOT_LOAD_PIPELINE.getCode() == code
        || Pan.ERROR_LOADING_TRANSFORMS_PLUGINS.getCode() == code;
    }
  }

  public enum Kitchen {

    SUCCESS( 0, "The workflow ran without a problem" ),
    ERRORS_DURING_PROCESSING( 1, "Errors occurred during processing" ),
    UNEXPECTED_ERROR( 2, "An unexpected error occurred during loading or running of the workflow" ),
    HOP_VERSION_PRINT( 6, "Hop Version printing" ),
    COULD_NOT_LOAD_JOB( 7, "The workflow couldn't be de-serialized" ),
    ERROR_LOADING_TRANSFORMS_PLUGINS( 8, "Error loading transforms or plugins (error in loading one of the plugins mostly)" ),
    CMD_LINE_PRINT( 9, "Command line usage printing" );

    private int code;
    private String description;

    Kitchen( int code, String description ) {
      setCode( code );
      setDescription( description );
    }

    public int getCode() {
      return code;
    }

    public void setCode( int code ) {
      this.code = code;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription( String description ) {
      this.description = description;
    }

    public static Kitchen getByCode( final int code ) {
      return Arrays.asList( Kitchen.values() ).stream()
        .filter( kitchen -> kitchen.getCode() == code )
        .findAny().orElse( null );
    }

    public static boolean isFailedExecution( final int code ) {
      return Kitchen.UNEXPECTED_ERROR.getCode() == code
        || Kitchen.COULD_NOT_LOAD_JOB.getCode() == code
        || Kitchen.ERROR_LOADING_TRANSFORMS_PLUGINS.getCode() == code;
    }
  }
}
