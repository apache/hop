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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class XsdValidatorData extends BaseTransformData implements ITransformData {
  public int xmlindex;
  public int xsdindex;
  public IRowMeta outputRowMeta;

  /** The most XSD file names whose compiled schema is kept per copy. */
  public static final int MAX_CACHED_SCHEMAS = 100;

  /** Created on first use and reused: creating a SchemaFactory scans the classpath. */
  public SchemaFactory schemaFactory;

  /** The schema without an XSD file, which uses the location hints in each document. */
  public Schema schemaFromDocuments;

  /** One validator per compiled schema, configured once. */
  public final Map<Schema, Validator> validators = new IdentityHashMap<>();

  /** Compiled schemas by XSD file name, the least recently used dropped beyond the limit. */
  public final Map<String, Schema> schemas =
      new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Schema> eldest) {
          if (size() > MAX_CACHED_SCHEMAS) {
            validators.remove(eldest.getValue());
            return true;
          }
          return false;
        }
      };

  public XsdValidatorData() {
    super();
    xmlindex = -1;
    xsdindex = -1;
  }
}
