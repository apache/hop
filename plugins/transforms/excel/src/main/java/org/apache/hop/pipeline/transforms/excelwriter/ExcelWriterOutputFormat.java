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

package org.apache.hop.pipeline.transforms.excelwriter;

public final class ExcelWriterOutputFormat {

  public static final String EXT_XLS = "xls";
  public static final String EXT_XLSX = "xlsx";
  public static final String EXT_ODS = "ods";

  private ExcelWriterOutputFormat() {}

  public static boolean isOds(String extension) {
    return EXT_ODS.equalsIgnoreCase(extension);
  }

  public static boolean isXlsx(String extension) {
    return EXT_XLSX.equalsIgnoreCase(extension);
  }

  public static boolean isXls(String extension) {
    return EXT_XLS.equalsIgnoreCase(extension);
  }
}
