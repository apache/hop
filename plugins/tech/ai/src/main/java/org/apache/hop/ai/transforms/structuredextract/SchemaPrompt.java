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
package org.apache.hop.ai.transforms.structuredextract;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Describes the wanted fields in plain text for the prompt.
 *
 * <p>This is sent even to models that accept a real JSON schema. The schema constrains the shape;
 * the descriptions say what the fields mean, and a model that can read "the date the contract
 * renews" does noticeably better than one given only a field called {@code renewal_date}.
 */
public final class SchemaPrompt {

  private SchemaPrompt() {}

  public static String describe(List<StructuredExtractField> fields) throws HopException {
    StringBuilder text = new StringBuilder("Fields:\n");
    for (StructuredExtractField field : fields) {
      if (field == null || field.trimmedName().isEmpty()) {
        continue;
      }
      text.append("- ").append(field.trimmedName()).append(": ").append(jsonTypeOf(field));
      if (!field.isRequired()) {
        text.append(", optional");
      }
      List<String> allowed = ExtractionSchema.allowedValues(field);
      if (!allowed.isEmpty()) {
        text.append(", one of ").append(String.join(", ", allowed));
      }
      if (!Utils.isEmpty(field.getDescription())) {
        text.append(". ").append(field.getDescription().trim());
      }
      text.append('\n');
    }
    return text.toString();
  }

  /** How the field should look in JSON, in words a model reads rather than Hop's type names. */
  private static String jsonTypeOf(StructuredExtractField field) throws HopException {
    return switch (ExtractionSchema.typeOf(field)) {
      case IValueMeta.TYPE_INTEGER -> "a whole number";
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER -> "a number";
      case IValueMeta.TYPE_BOOLEAN -> "true or false";
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP ->
          ExtractionSchema.dateFormatDescription(field);
      default -> "text";
    };
  }
}
