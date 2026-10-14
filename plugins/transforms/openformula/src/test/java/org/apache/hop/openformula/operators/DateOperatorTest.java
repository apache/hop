/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.openformula.operators;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.TimeZone;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Creation-Date: 10.04.2007, 15:31:58
 *
 * @author Thomas Morgner
 */
public class DateOperatorTest extends FormulaTestBase {

  private TimeZone origTz;

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {
        "DATETIMEVALUE(\"2009-10-10T17:15:00.00+0000\") + 0",
        new BigDecimal("40094.7604166666666666666666666666666667")
      },
      {
        "DATETIMEVALUE(\"2009-10-10T17:30:00.00+0000\") - 0",
        new BigDecimal("40094.7708333333333333333333333333333333")
      },
      {
        "DATETIMEVALUE(\"2009-10-10T17:30:00.00+0000\") - DATETIMEVALUE(\"2009-10-10T17:30:00.00+0000\")",
        new BigDecimal(0)
      },
      {
        "DATETIMEVALUE(\"2009-10-10T17:30:00.00+0000\") - DATETIMEVALUE(\"2009-10-10T17:15:00.00+0000\")",
        new BigDecimal("0.0104166666666666666666666666666667")
      },
    };
  }

  public DateOperatorTest() {}

  @BeforeEach
  public void setUp() throws Exception {
    origTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+01:00"));
    super.setUp();
  }

  @AfterEach
  public void tearDown() throws Exception {
    TimeZone.setDefault(origTz);
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }

  @Test
  public void testDefaultTZ() throws Exception {
    runDefaultTest();
  }
}
