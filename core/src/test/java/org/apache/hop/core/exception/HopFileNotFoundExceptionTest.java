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

package org.apache.hop.core.exception;

import org.apache.hop.core.Const;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HopFileNotFoundExceptionTest {

  private String expectedNullMessage = Const.CR + "null" + Const.CR;
  private String errorMessage = "error message";
  private String causeExceptionMessage = "Cause exception";
  private String filepath = "file.txt";
  private Throwable cause = new RuntimeException( causeExceptionMessage );

  @Test
  public void testConstructor() {
    try {
      throw new HopFileNotFoundException();
    } catch ( HopFileNotFoundException e ) {
      assertEquals( null, e.getCause() );
      assertTrue( e.getMessage().contains( expectedNullMessage ) );
      assertEquals( null, e.getFilepath() );
    }
  }

  @Test
  public void testConstructorMessage() {
    try {
      throw new HopFileNotFoundException( errorMessage );
    } catch ( HopFileNotFoundException e ) {
      assertEquals( null, e.getCause() );
      assertTrue( e.getMessage().contains( errorMessage ) );
      assertEquals( null, e.getFilepath() );
    }
  }

  @Test
  public void testConstructorMessageAndFilepath() {
    try {
      throw new HopFileNotFoundException( errorMessage, filepath );
    } catch ( HopFileNotFoundException e ) {
      assertEquals( null, e.getCause() );
      assertTrue( e.getMessage().contains( errorMessage ) );
      assertEquals( filepath, e.getFilepath() );
    }
  }

  @Test
  public void testConstructorThrowable() {
    try {
      throw new HopFileNotFoundException( cause );
    } catch ( HopFileNotFoundException e ) {
      assertEquals( cause, e.getCause() );
      assertTrue( e.getMessage().contains( causeExceptionMessage ) );
      assertEquals( null, e.getFilepath() );
    }
  }

  @Test
  public void testConstructorMessageAndThrowable() {
    Throwable cause = new RuntimeException( causeExceptionMessage );
    try {
      throw new HopFileNotFoundException( errorMessage, cause );
    } catch ( HopFileNotFoundException e ) {
      assertTrue( e.getMessage().contains( errorMessage ) );
      assertTrue( e.getMessage().contains( causeExceptionMessage ) );
      assertEquals( cause, e.getCause() );
      assertEquals( null, e.getFilepath() );
    }
  }
}
