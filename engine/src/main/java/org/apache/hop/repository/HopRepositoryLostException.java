/*! ******************************************************************************
 *
 *
 * Pentaho Data Integration
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

package org.apache.hop.repository;

import org.apache.hop.i18n.BaseMessages;

@SuppressWarnings( "serial" )
public class HopRepositoryLostException extends RuntimeException {

  private static Class<?> PKG = HopRepositoryLostException.class;
  private static final String MSG = BaseMessages.getString( PKG, "Repository.Lost.Error.Message" );
  private static final String PREFACE = BaseMessages.getString( PKG, "Repository.Lost.Error.Preface" );

  public HopRepositoryLostException() {
    super( MSG );
  }

  public HopRepositoryLostException( String message ) {
    super( message );
  }

  public HopRepositoryLostException( Throwable cause ) {
    super( MSG, cause );
  }

  public HopRepositoryLostException( String message, Throwable cause ) {
    super( message, cause );
  }

  public static HopRepositoryLostException lookupStackStrace( Throwable root ) {
    while ( root != null ) {
      if ( root instanceof HopRepositoryLostException ) {
        return (HopRepositoryLostException) root;
      } else {
        root = root.getCause();
      }
    }

    return null;
  }

  /*
   * According to UX the verbiage to be displayed to user
   * should consist of 2 parts:
   * the first one is in Exception message
   * the second one is in the Preface.
   */
  public String getPrefaceMessage() {
    return PREFACE;
  }
}
