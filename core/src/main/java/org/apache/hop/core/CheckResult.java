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

import org.apache.hop.i18n.BaseMessages;

/**
 * This class is used to store results of pipeline and transform verifications.
 *
 * @author Matt
 * @since 11-01-04
 */
public class CheckResult implements ICheckResult {
  private static final Class<?> PKG = Const.class; // For Translator

  public static final String[] typeDesc = {
    "", BaseMessages.getString( PKG, "CheckResult.OK" ), BaseMessages.getString( PKG, "CheckResult.Remark" ),
    BaseMessages.getString( PKG, "CheckResult.Warning" ), BaseMessages.getString( PKG, "CheckResult.Error" ) };

  private int type;

  private String text;

  // MB - Support both JobEntry and Transform Checking
  // 6/26/07
  private ICheckResultSource sourceMeta;

  private String errorCode;

  public CheckResult() {
    this( ICheckResult.TYPE_RESULT_NONE, "", null );
  }

  public CheckResult( int t, String s, ICheckResultSource sourceMeta ) {
    type = t;
    text = s;
    this.sourceMeta = sourceMeta;
  }

  public CheckResult( int t, String errorCode, String s, ICheckResultSource sourceMeta ) {
    this( t, s, sourceMeta );
    this.errorCode = errorCode;
  }

  @Override
  public int getType() {
    return type;
  }

  @Override
  public String getTypeDesc() {
    return typeDesc[ type ];
  }

  @Override
  public String getText() {
    return text;
  }

  @Override
  public ICheckResultSource getSourceInfo() {
    return sourceMeta;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append( typeDesc[ type ] ).append( ": " ).append( text );

    if ( sourceMeta != null ) {
      sb.append( " (" ).append( sourceMeta.getName() ).append( ")" );
    }

    return sb.toString();
  }

  /**
   * @return the errorCode
   */
  @Override
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @param errorCode the errorCode to set
   */
  @Override
  public void setErrorCode( String errorCode ) {
    this.errorCode = errorCode;
  }

  @Override
  public void setText( String value ) {
    this.text = value;
  }

  @Override
  public void setType( int value ) {
    this.type = value;
  }

}
