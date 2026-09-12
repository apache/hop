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

package org.apache.hop.ai.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Lists project metadata for the inclusion picker and serializes the user's selection into a
 * redacted JSON block for the advisory prompt.
 */
public final class AiAdvisorMetadataContext {

  static final int MAX_METADATA_CHARS = 80_000;
  static final int MAX_OBJECT_CHARS = 20_000;

  private AiAdvisorMetadataContext() {}

  public static void appendToPrompt(StringBuilder prompt, AiAdvisorRequest request) {
    if (prompt == null || request == null) {
      return;
    }
    if (!request.inclusionEnabled(AiAdvisorInclusions.METADATA)) {
      return;
    }
    if (request.getMetadataProvider() == null
        || request.getMetadataSelections() == null
        || request.getMetadataSelections().isEmpty()) {
      return;
    }
    prompt
        .append("Selected metadata JSON:\n")
        .append(serialize(request.getMetadataProvider(), request.getMetadataSelections()))
        .append("\n\n");
  }

  @SuppressWarnings("unchecked")
  public static String serialize(
      IHopMetadataProvider metadataProvider, List<AiAdvisorMetadataSelection> selections) {
    JSONArray elements = new JSONArray();
    if (metadataProvider == null || selections == null) {
      return "{\"elements\":[]}";
    }
    int total = 0;
    for (AiAdvisorMetadataSelection selection : selections) {
      if (selection == null
          || Utils.isEmpty(selection.getTypeKey())
          || Utils.isEmpty(selection.getName())) {
        continue;
      }
      JSONObject element = serializeOne(metadataProvider, selection);
      String piece = element.toJSONString();
      if (piece.length() > MAX_OBJECT_CHARS) {
        element.put("content", null);
        element.put("truncated", Boolean.TRUE);
        piece = element.toJSONString();
      }
      if (total + piece.length() > MAX_METADATA_CHARS) {
        JSONObject note = new JSONObject();
        note.put("truncated", Boolean.TRUE);
        note.put("reason", "metadata context exceeded size cap");
        elements.add(note);
        break;
      }
      elements.add(element);
      total += piece.length();
    }
    JSONObject root = new JSONObject();
    root.put("elements", elements);
    return AiTextUtil.redactSecrets(root.toJSONString());
  }

  @SuppressWarnings("unchecked")
  static JSONObject serializeOne(
      IHopMetadataProvider metadataProvider, AiAdvisorMetadataSelection selection) {
    JSONObject element = new JSONObject();
    element.put("type", selection.getTypeKey());
    element.put("name", selection.getName());
    try {
      Class<IHopMetadata> metadataClass =
          metadataProvider.getMetadataClassForKey(selection.getTypeKey());
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
      if (annotation != null) {
        element.put("typeName", typeLabel(metadataClass, annotation));
      }
      IHopMetadataSerializer<IHopMetadata> serializer =
          metadataProvider.getSerializer(metadataClass);
      IHopMetadata object = serializer.load(selection.getName());
      if (object == null) {
        element.put("error", "not found");
        return element;
      }
      JsonMetadataParser<IHopMetadata> parser =
          new JsonMetadataParser<>(metadataClass, metadataProvider);
      element.put("content", parser.getJsonObject(object));
    } catch (Exception e) {
      element.put("error", Const.NVL(e.getMessage(), e.getClass().getSimpleName()));
    }
    return element;
  }

  public static List<TypeCatalog> listTypes(IHopMetadataProvider metadataProvider) {
    List<TypeCatalog> types = new ArrayList<>();
    if (metadataProvider == null) {
      return types;
    }
    for (Class<IHopMetadata> metadataClass : metadataProvider.getMetadataClasses()) {
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
      if (annotation == null || Utils.isEmpty(annotation.key())) {
        continue;
      }
      try {
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = new ArrayList<>(serializer.listObjectNames());
        names.sort(String.CASE_INSENSITIVE_ORDER);
        if (names.isEmpty()) {
          continue;
        }
        types.add(new TypeCatalog(annotation.key(), typeLabel(metadataClass, annotation), names));
      } catch (Exception e) {
        // Skip types we cannot list (missing plugin, corrupt store).
      }
    }
    types.sort(Comparator.comparing(TypeCatalog::getTypeName, String.CASE_INSENSITIVE_ORDER));
    return types;
  }

  static String typeLabel(Class<IHopMetadata> metadataClass, HopMetadata annotation) {
    String translated = TranslateUtil.translate(annotation.name(), metadataClass);
    return Utils.isEmpty(translated) ? annotation.key() : translated;
  }

  @Getter
  @AllArgsConstructor
  public static final class TypeCatalog {
    private final String typeKey;
    private final String typeName;
    private final List<String> names;
  }
}
