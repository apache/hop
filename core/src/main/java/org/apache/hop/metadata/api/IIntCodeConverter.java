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
 *
 */

package org.apache.hop.metadata.api;

/**
 * The class that implements this interface can convert between an integer and a code string. This
 * is used to convert from old-style integer codes to their string code equivalent. We can use it
 * primarily to convert from the integer ID of value metadata to the String form of the data type.
 */
public interface IIntCodeConverter {
  String getCode(int type);

  int getType(String code);

  class None implements IIntCodeConverter {
    @Override
    public String getCode(int type) {
      throw new RuntimeException("This is not intended to be used");
    }

    @Override
    public int getType(String code) {
      throw new RuntimeException("This is not intended to be used");
    }
  }
}
