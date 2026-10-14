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

package org.apache.hop.openformula.function.datetime;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.apache.hop.openformula.Formula;
import org.apache.hop.openformula.common.FormulaTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Cedric Pronzato
 */
public class DateFunctionTest extends FormulaTestBase {

  private TimeZone defaultTz;

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    defaultTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    TimeZone.setDefault(defaultTz);
  }

  @Override
  public Object[][] createDataTest() {
    return new Object[][] {
      {"DATE(2005;1;31)=[.C7]", Boolean.TRUE},
      {"DATE(2005;12;31)-DATE(1904;1;1)", new BigDecimal(37255)},
      {"DATE(2004;2;29)=DATE(2004;2;28)+1", Boolean.TRUE},
      {"DATE(2000;2;29)=DATE(2000;2;28)+1", Boolean.TRUE},
      {"DATE(2005;3;1)=DATE(2005;2;28)+1", Boolean.TRUE},
      {"DATE(2017.5; 1;2)=DATE(2017; 1; 2)", Boolean.TRUE},
      {"DATE(2006; 2.5;3)=DATE(2006; 2; 3)", Boolean.TRUE},
      {"DATE(2006; 1;3.5)=DATE(2006; 1; 3)", Boolean.TRUE},
      {"DATE(2006; 13; 3)=DATE(2007;1; 3)", Boolean.TRUE},
      {"DATE(2006; 1; 32)=DATE(2006;2; 1)", Boolean.TRUE},
      {"DATE(2006; 25;34)=DATE(2008;2;3)", Boolean.TRUE},
      {"DATE(2006;-1;1)=DATE(2005;11;1)", Boolean.TRUE},
      {"DATE(2006;4;-1)=DATE(2006;3;30)", Boolean.TRUE},
      {"DATE(2006;-4;-1)=DATE(2005;7;30)", Boolean.TRUE},
      {"DATE(2003;2;29)=DATE(2003;3;1)", Boolean.TRUE},
    };
  }

  @Test
  public void testDebugDates() throws Exception {
    Formula formula = null;
    formula = new Formula("DATE(2005;1;31)");
    formula.initialize(getContext());
    Date eval = (Date) formula.evaluate();
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    System.out.println(eval.getClass().getName() + ":" + df.format(eval) + ";" + eval.getTime());

    final Date date = (Date) getContext().resolveReference(".C7");
    System.out.println(df.format(date) + ";" + date.getTime());
    assertEquals(date, eval);
  }

  @Test
  public void testGregorianCalendar() {
    final Date d1 = new GregorianCalendar(2006, -1, 1).getTime();
    final Date d2 = new GregorianCalendar(2005, 11, 1).getTime();

    assertEquals(d1, d2);
  }

  @Test
  public void testDefault() throws Exception {
    runDefaultTest();
  }
}
