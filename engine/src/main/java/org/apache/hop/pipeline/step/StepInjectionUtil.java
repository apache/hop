/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
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

package org.apache.hop.pipeline.step;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;

import java.util.List;

public class StepInjectionUtil {

  public static StepInjectionMetaEntry getEntry( StepMetaInjectionEntryInterface entryInterface ) {
    return getEntry( entryInterface, null );
  }

  public static StepInjectionMetaEntry getEntry( StepMetaInjectionEntryInterface entryInterface, Object value ) {
    StepInjectionMetaEntry entry = new StepInjectionMetaEntry(
      entryInterface.name(),
      entryInterface.getValueType(),
      entryInterface.getDescription() );

    // If the value is null, leave it alone.
    //
    if ( value == null ) {
      return entry;
    }
    switch ( entryInterface.getValueType() ) {
      case ValueMetaInteger.TYPE_STRING:
        if ( value instanceof Boolean ) {
          entry.setValue( ( (Boolean) value ) ? "Y" : "N" );
        } else {
          entry.setValue( value.toString() );
        }
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        entry.setValue( Long.valueOf( value.toString() ) );
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        entry.setValue( "Y".equalsIgnoreCase( value.toString() )
          || "TRUE".equalsIgnoreCase( value.toString() ) );
        break;
      case ValueMetaInterface.TYPE_NONE:
        break;
      default:
        entry.setValue( value );
        break;
    }
    return entry;
  }

  public static StepInjectionMetaEntry findEntry( List<StepInjectionMetaEntry> entries,
                                                  StepMetaInjectionEntryInterface match ) {
    return findEntry( entries, match.name() );
  }

  public static StepInjectionMetaEntry findEntry( List<StepInjectionMetaEntry> entries,
                                                  String key ) {
    for ( StepInjectionMetaEntry entry : entries ) {
      if ( entry.getKey().equals( key ) ) {
        return entry;
      }
    }
    return null;
  }

  /**
   * This method compares 2 sets of step injection meta entries. They have to be in the same order.
   * We will traverse into nested details.
   *
   * @param refEntries The reference list
   * @param cmpEntries The list to compare
   * @throws HopException
   */
  public static void compareEntryValues( List<StepInjectionMetaEntry> refEntries,
                                         List<StepInjectionMetaEntry> cmpEntries ) throws HopException {

    if ( refEntries.size() != cmpEntries.size() ) {
      throw new HopException( "The number of reference entries (" + refEntries.size()
        + ") is not the same as the number of compare entries(" + cmpEntries.size() + ")" );
    }

    for ( int i = 0; i < refEntries.size(); i++ ) {
      StepInjectionMetaEntry refEntry = refEntries.get( i );
      StepInjectionMetaEntry cmpEntry = cmpEntries.get( i );
      if ( cmpEntry.getValueType() == ValueMetaInteger.TYPE_NONE ) {
        compareEntryValues( refEntry.getDetails(), cmpEntry.getDetails() );
      } else {
        Object ref = refEntry.getValue();
        Object cmp = cmpEntry.getValue();
        if ( ref != null && cmp == null ) {
          throw new HopException( "Reference key '" + refEntry.getKey()
            + "': value is not null while the compare value is null" );
        }
        if ( ref == null && cmp != null ) {
          throw new HopException( "Reference key '" + refEntry.getKey()
            + "': value is null while the compare value is not null" );
        }
        if ( ref != null && cmp != null ) {
          if ( !ref.equals( cmp ) ) {
            throw new HopException( "Reference key '" + refEntry.getKey()
              + "': reference value '" + ref + "' is not equal to '" + cmp + "'" );
          }
        }
      }
    }
  }
}
