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

package org.apache.hop.trans;

import junit.framework.TestCase;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public abstract class TransformationIT extends TestCase {

  public TransformationIT() throws HopException {
    super();
    HopEnvironment.init( false );
  }

  public TransformationIT( String name ) throws HopException {
    super( name );
    HopEnvironment.init( false );
  }

  public RowMetaInterface createRowMetaInterface( ValueMetaInterface... valueMetas ) {
    RowMetaInterface rm = new RowMeta();

    for ( ValueMetaInterface vm : valueMetas ) {
      rm.addValueMeta( vm );
    }

    return rm;
  }

  public List<RowMetaAndData> createData( RowMetaInterface rm, Object[][] rows ) {
    List<RowMetaAndData> list = new ArrayList<RowMetaAndData>();

    for ( Object[] row : rows ) {
      list.add( new RowMetaAndData( rm, row ) );
    }

    return list;
  }

  /**
   * Check the 2 lists comparing the rows in order. If they are not the same fail the test.
   */
  public void checkRows( List<RowMetaAndData> rows1, List<RowMetaAndData> rows2 ) {
    if ( rows1.size() != rows2.size() ) {
      fail( "Number of rows is not the same: " + rows1.size() + " and " + rows2.size() );
    }
    ListIterator<RowMetaAndData> it1 = rows1.listIterator();
    ListIterator<RowMetaAndData> it2 = rows2.listIterator();

    while ( it1.hasNext() && it2.hasNext() ) {
      RowMetaAndData rm1 = it1.next();
      RowMetaAndData rm2 = it2.next();

      Object[] r1 = rm1.getData();
      Object[] r2 = rm2.getData();

      if ( rm1.size() != rm2.size() ) {
        fail( "row nr " + it1.nextIndex() + " is not equal" );
      }
      int[] fields = new int[ r1.length ];
      for ( int ydx = 0; ydx < r1.length; ydx++ ) {
        fields[ ydx ] = ydx;
      }
      try {
        if ( rm1.getRowMeta().compare( r1, r2, fields ) != 0 ) {
          fail( "row nr " + it1.nextIndex() + " is not equal" );
        }
      } catch ( HopValueException e ) {
        fail( "row nr " + it1.nextIndex() + " is not equal" );
      }
    }
  }
}
