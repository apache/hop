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

package org.apache.hop.staticschema.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.staticschema.metadata.SchemaDefinition;

public class SchemaDefinitionUtil {

  public SchemaDefinition loadSchemaDefinition(
      IHopMetadataProvider metadataProvider, String schemaDefinitionName)
      throws HopTransformException {
    if (StringUtils.isEmpty(schemaDefinitionName)) {
      throw new HopTransformException("No schema definition name provided");
    }
    SchemaDefinition schemaDefinition;
    try {
      IHopMetadataSerializer<SchemaDefinition> serializer =
          metadataProvider.getSerializer(SchemaDefinition.class);
      schemaDefinition = serializer.load(schemaDefinitionName);
    } catch (Exception e) {
      throw new HopTransformException(
          "Unable to schema definition '" + schemaDefinitionName + "' from the metadata", e);
    }
    if (schemaDefinition == null) {
      throw new HopTransformException(
          "Unable to find schema definition '" + schemaDefinitionName + "' in the metadata");
    }

    try {
      schemaDefinition.validate();
    } catch (Exception e) {
      throw new HopTransformException(
          "There was an error validating file definition " + schemaDefinition.getName(), e);
    }

    return schemaDefinition;
  }
}
