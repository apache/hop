/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.apache.hop.metastore.test;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.xml.XmlMetaStore;
import org.apache.hop.metastore.util.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class XmlMetaStoreIT extends MetaStoreTestBase {

  private XmlMetaStore metaStore;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    metaStore = createMetaStore();
  }

  protected XmlMetaStore createMetaStore() throws MetaStoreException {
    return new XmlMetaStore();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    FileUtil.cleanFolder( new File( metaStore.getRootFolder() ).getParentFile(), true );
  }

  public void test() throws Exception {
    super.testFunctionality( metaStore );
  }

  public void testParallelDifferentStores() throws Exception {
    List<XmlMetaStore> stores = new ArrayList<XmlMetaStore>();
    final List<Throwable> exceptions = new ArrayList<Throwable>();
    // Run the test against the XML metadata store.
    //
    try {
      for ( int i = 0; i < 10; i++ ) {
        stores.add( new XmlMetaStore() );
      }

      List<Thread> threads = new ArrayList<Thread>();
      for ( final IMetaStore store : stores ) {
        Thread thread = new Thread() {
          public void run() {
            try {
              testFunctionality( store );
            } catch ( Throwable e ) {
              exceptions.add( e );
            }
          }
        };
        threads.add( thread );
        thread.start();
      }

      for ( Thread thread : threads ) {
        thread.join();
      }
    } finally {
      for ( XmlMetaStore store : stores ) {
        FileUtil.cleanFolder( new File( store.getRootFolder() ).getParentFile(), true );
      }
    }

    if ( !exceptions.isEmpty() ) {
      for ( Throwable e : exceptions ) {
        e.printStackTrace( System.err );
      }
      fail( exceptions.size() + " exceptions encountered during parallel store/retrieve" );
    }

  }

  public void testParallelOneStore() throws Exception {
    final List<Exception> exceptions = new ArrayList<Exception>();

    List<Thread> threads = new ArrayList<Thread>();

    for ( int i = 9000; i < 9020; i++ ) {
      final int index = i;
      Thread thread = new Thread() {
        public void run() {
          try {
            parallelStoreRetrieve( metaStore, index );
          } catch ( Exception e ) {
            exceptions.add( e );
          }
        }
      };
      threads.add( thread );
      thread.start();
    }

    for ( Thread thread : threads ) {
      thread.join();
    }

    if ( !exceptions.isEmpty() ) {
      for ( Exception e : exceptions ) {
        e.printStackTrace( System.err );
      }
      fail( exceptions.size() + " exceptions encountered during parallel store/retrieve" );

    }
  }

}

