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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

/** Built-in card scheme rules used to validate card length independently of the BIN file. */
public final class CardScheme {

  private CardScheme() {}

  /** Checks whether the given card-number length is valid for the given BIN prefix. */
  public static boolean isValidLength(String bin, int cardLength) {
    String b = bin == null ? "" : bin;
    if (b.startsWith("4")) {
      // Visa: 13, 16 or 19
      return cardLength == 13 || cardLength == 16 || cardLength == 19;
    }
    if (b.startsWith("34") || b.startsWith("37")) {
      // American Express: 15
      return cardLength == 15;
    }
    if (b.startsWith("6011") || b.startsWith("65")) {
      // Discover (6011, 65): 16 to 19
      return cardLength >= 16 && cardLength <= 19;
    }
    if (b.startsWith("644")
        || b.startsWith("645")
        || b.startsWith("646")
        || b.startsWith("647")
        || b.startsWith("648")
        || b.startsWith("649")) {
      // Discover (644-649): 16 to 19
      return cardLength >= 16 && cardLength <= 19;
    }
    if (b.length() >= 3 && b.charAt(0) == '6' && b.charAt(1) == '2') {
      // Discover (622126-622925): 16 to 19
      return cardLength >= 16 && cardLength <= 19;
    }
    if (inRange(b, "2221", "2720")) {
      // Mastercard (2221-2720): 16
      return cardLength == 16;
    }
    if (inRange(b, "51", "55")) {
      // Mastercard (51-55): 16
      return cardLength == 16;
    }
    if (inRange(b, "3528", "3589")) {
      // JCB (3528-3589): 16 to 19
      return cardLength >= 16 && cardLength <= 19;
    }
    if (b.startsWith("300")
        || b.startsWith("301")
        || b.startsWith("302")
        || b.startsWith("303")
        || b.startsWith("304")
        || b.startsWith("305")
        || b.startsWith("3095")
        || b.startsWith("36")
        || b.startsWith("38")
        || b.startsWith("39")) {
      // Diners Club: 14 to 19
      return cardLength >= 14 && cardLength <= 19;
    }
    if (b.startsWith("62")) {
      // UnionPay: 16 to 19
      return cardLength >= 16 && cardLength <= 19;
    }
    if (b.startsWith("5018")
        || b.startsWith("5020")
        || b.startsWith("5038")
        || b.startsWith("5893")
        || b.startsWith("6304")
        || b.startsWith("6759")
        || b.startsWith("6761")
        || b.startsWith("6762")
        || b.startsWith("6763")) {
      // Maestro: 12 to 19
      return cardLength >= 12 && cardLength <= 19;
    }
    if (inRange(b, "506099", "506198")
        || inRange(b, "507865", "507964")
        || inRange(b, "650002", "650027")) {
      // Verve: 16, 18 or 19
      return cardLength == 16 || cardLength == 18 || cardLength == 19;
    }
    // Unknown scheme: accept any length.
    return true;
  }

  private static boolean inRange(String bin, String min, String max) {
    int prefixLength = min.length();
    if (bin.length() < prefixLength) {
      return false;
    }
    String prefix = bin.substring(0, prefixLength);
    return prefix.compareTo(min) >= 0 && prefix.compareTo(max) <= 0;
  }
}
