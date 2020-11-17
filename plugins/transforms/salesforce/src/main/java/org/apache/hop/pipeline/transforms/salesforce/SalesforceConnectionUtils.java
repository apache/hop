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

package org.apache.hop.pipeline.transforms.salesforce;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.salesforceinput.SalesforceInputMeta;

public class SalesforceConnectionUtils {

  public static final int MAX_UPDATED_OBJECTS_IDS = 2000;

  private static Class<?> PKG = SalesforceInputMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String TARGET_DEFAULT_URL = "https://login.salesforce.com/services/Soap/u/50.0";

  public static final String DEFAULT_TIMEOUT = "60000";

  /**
   * The records filter description
   */
  public static final String[] recordsFilterDesc = {
    BaseMessages.getString( PKG, "SalesforceInputMeta.recordsFilter.All" ),
    BaseMessages.getString( PKG, "SalesforceInputMeta.recordsFilter.Updated" ),
    BaseMessages.getString( PKG, "SalesforceInputMeta.recordsFilter.Deleted" ) };

  /**
   * The records filter type codes
   */
  public static final String[] recordsFilterCode = { "all", "updated", "deleted" };

  public static final int RECORDS_FILTER_ALL = 0;

  public static final int RECORDS_FILTER_UPDATED = 1;

  public static final int RECORDS_FILTER_DELETED = 2;

  public static String getRecordsFilterDesc( int i ) {
    if ( i < 0 || i >= recordsFilterDesc.length ) {
      return recordsFilterDesc[0];
    }
    return recordsFilterDesc[i];
  }

  public static int getRecordsFilterByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < recordsFilterDesc.length; i++ ) {
      if ( recordsFilterDesc[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getRecordsFilterByCode( tt );
  }

  public static int getRecordsFilterByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < recordsFilterCode.length; i++ ) {
      if ( recordsFilterCode[i].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static String getRecordsFilterCode( int i ) {
    if ( i < 0 || i >= recordsFilterCode.length ) {
      return recordsFilterCode[0];
    }
    return recordsFilterCode[i];
  }

}
