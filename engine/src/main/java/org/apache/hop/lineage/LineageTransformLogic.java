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

package org.apache.hop.lineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.api.IDeclaresTransformLogic;
import org.apache.hop.lineage.api.IDeclaresTransformLogic.FieldLogic;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * The logic a transform applies to its fields ({@link IDeclaresTransformLogic}), as an attribute of
 * its lifecycle events, for the sinks to show with the transform.
 */
public final class LineageTransformLogic {

  /** The attribute that holds the logic, a JSON array of {@code field, language, expression}. */
  public static final String ATTRIBUTE = "transformLogic";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<FieldLogic>> TYPE = new TypeReference<>() {};

  private LineageTransformLogic() {}

  /** Adds the logic of a transform that declares it. Nothing for one that doesn't, or has none. */
  public static void put(
      LineageContext.Builder ctx, TransformMeta transformMeta, IVariables variables) {
    if (transformMeta == null
        || !(transformMeta.getTransform() instanceof IDeclaresTransformLogic declares)) {
      return;
    }
    try {
      List<FieldLogic> logic = new ArrayList<>();
      for (FieldLogic field : declares.transformLogic(variables)) {
        if (field != null && !Utils.isEmpty(field.field()) && !Utils.isEmpty(field.expression())) {
          logic.add(field);
        }
      }
      if (!logic.isEmpty()) {
        ctx.putAttribute(ATTRIBUTE, MAPPER.writeValueAsString(logic));
      }
    } catch (Exception e) {
      // The logic is extra information: a transform that can't tell it is shown without it
    }
  }

  /** The logic in a context's attributes; empty when there is none or it can't be read. */
  public static List<FieldLogic> get(Map<String, String> attributes) {
    String json = attributes == null ? null : attributes.get(ATTRIBUTE);
    if (Utils.isEmpty(json)) {
      return List.of();
    }
    try {
      List<FieldLogic> logic = MAPPER.readValue(json, TYPE);
      return logic == null ? List.of() : logic;
    } catch (JsonProcessingException e) {
      return List.of();
    }
  }
}
