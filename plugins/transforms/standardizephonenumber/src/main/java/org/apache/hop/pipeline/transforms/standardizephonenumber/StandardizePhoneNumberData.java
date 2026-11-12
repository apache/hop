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

package org.apache.hop.pipeline.transforms.standardizephonenumber;

import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class StandardizePhoneNumberData extends BaseTransformData implements ITransformData {

  protected IRowMeta outputRowMeta;

  /** The settings of each configured field, resolved on the first row. */
  protected List<Field> fields;

  /** One configured field: names resolved, positions in the row looked up. */
  static final class Field {
    String inputField;
    int inputIndex;
    int outputIndex;

    /** Position of the country field in the input row, -1 when there is none. */
    int countryIndex;

    /** The region to use when the row has no country, or an unknown one. */
    String defaultRegion;

    String numberFormat;

    /** The format for {@link #numberFormat}, set when the first number is formatted. */
    PhoneNumberFormat format;

    /** Position of the number type field in the output row, -1 when there is none. */
    int numberTypeIndex;

    /** Position of the validity field in the output row, -1 when there is none. */
    int isValidIndex;
  }

  public StandardizePhoneNumberData() {
    super();
  }
}
