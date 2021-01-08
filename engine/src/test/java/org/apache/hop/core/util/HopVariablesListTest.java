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

package org.apache.hop.core.util;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopVariablesList;
import org.apache.hop.core.config.DescribedVariable;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by Yury_Bakhmutski on 11/4/2015.
 */
public class HopVariablesListTest {

  @Test
  public void testInit() throws Exception {
    HopVariablesList variablesList = HopVariablesList.getInstance();
    variablesList.init();

    DescribedVariable describedVariable = variablesList.findEnvironmentVariable( Const.HOP_PASSWORD_ENCODER_PLUGIN );
    assertNotNull( describedVariable );

    boolean actual = Boolean.valueOf( describedVariable.getValue() );
    assertEquals( false, actual );

    assertEquals( "Specifies the password encoder plugin to use by ID (Hop is the default).", describedVariable.getDescription() );
  }

  @Test
  public void testInit_closeInputStream() throws Exception {
    HopVariablesList.init();
    RandomAccessFile fos = null;
    try {
      File file = new File( Const.HOP_VARIABLES_FILE );
      if ( file.exists() ) {
        fos = new RandomAccessFile( file, "rw" );
      }
    } catch ( FileNotFoundException | SecurityException e ) {
      fail( "the file with properties should be unallocated" );
    } finally {
      if ( fos != null ) {
        fos.close();
      }
    }
  }
}
