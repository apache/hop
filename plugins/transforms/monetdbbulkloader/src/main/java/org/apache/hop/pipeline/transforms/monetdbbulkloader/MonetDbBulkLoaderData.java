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
package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class MonetDbBulkLoaderData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger outputLogger;

  // MonetDB API
  public MapiSocket mserver;
  public BufferedMCLReader in;
  public BufferedMCLWriter out;

  public String quote; // fieldEnclosure in MonetDBBulkLoaderMeta
  public String separator; // fieldSeparator in MonetDBBulkLoaderMeta
  public String nullrepresentation; // NULLrepresentation in MonetDBBulkLoaderMeta
  public String newline; // receives value in the init(...) in MonetDBBulkLoader

  public IValueMeta monetDateMeta;
  public IValueMeta monetNumberMeta;

  public IValueMeta monetTimestampMeta;
  public IValueMeta monetTimeMeta;

  public int bufferSize;

  public String[] rowBuffer;

  public int bufferIndex;

  public String schemaTable;

  /** Default constructor. */
  public MonetDbBulkLoaderData() {
    super();

    db = null;
  }
}
