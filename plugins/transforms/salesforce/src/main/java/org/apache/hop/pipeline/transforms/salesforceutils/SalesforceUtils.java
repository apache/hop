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

package org.apache.hop.pipeline.transforms.salesforceutils;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;

import java.util.regex.Pattern;


/**
 * Utility class to process salesforce specific features.
 *
 * @author Tatsiana_Kasiankova
 *
 */
public class SalesforceUtils {

  private SalesforceUtils() {
  }

  private static Class<?> PKG = SalesforceUtils.class; // For Translator

  private static final String EXTID_SEPARATOR = "/";

  private static final String CUSTOM_OBJECT_RELATIONSHIP_FIELD_SUFFIX = "_r";

  private static final String CUSTOM_OBJECT_SUFFIX = "_c";

  private static final Pattern FIELD_NAME_WITH_EXTID_PATTERN = Pattern.compile( "^\\w+\\:\\w+\\/\\w+$" );

  /**
   * Extract and return the correct name for the field that should be processed as NULL
   *
   * @param log
   *          the logging object
   * @param field
   *          the field that should be processed as NULL
   * @param isUseExtId
   *          the flag that indicates if the field is external id or not
   * @return return the correct name for the field that should be processed as NULL
   */
  public static String getFieldToNullName( ILogChannel log, String field, boolean isUseExtId ) {
    String fieldToNullName = field;
    if ( isUseExtId ) {
      // verify if the field has correct syntax
      if ( !FIELD_NAME_WITH_EXTID_PATTERN.matcher( field ).matches() ) {
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "SalesforceUtils.Warn.IncorrectExternalKeySyntax", field,
              fieldToNullName ) );
        }
        return fieldToNullName;
      }

      String lookupField = field.substring( field.indexOf( EXTID_SEPARATOR ) + 1 );
      // working with custom objects and relationship
      // cut off _r and then add _c in the end of the name
      if ( lookupField.endsWith( CUSTOM_OBJECT_RELATIONSHIP_FIELD_SUFFIX ) ) {
        fieldToNullName =
            lookupField.substring( 0, lookupField.length() - CUSTOM_OBJECT_RELATIONSHIP_FIELD_SUFFIX.length() )
                + CUSTOM_OBJECT_SUFFIX;
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "SalesforceUtils.Debug.NullFieldName", fieldToNullName ) );
        }
        return fieldToNullName;
      }

      fieldToNullName = lookupField + "Id";
    }

    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "SalesforceUtils.Debug.NullFieldName", fieldToNullName ) );
    }

    return fieldToNullName;
  }

}
