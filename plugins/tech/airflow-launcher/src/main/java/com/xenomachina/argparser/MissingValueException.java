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

package com.xenomachina.argparser;

/**
 * Stands in for the class of the same name in kotlin-argparser, which is LGPL and which Hop does
 * not ship. The Airflow SDK's {@code Server} class refers to it, and the JVM loads it to verify
 * that class, even though Hop never calls the code that throws it. Written for Hop; it is never
 * thrown.
 *
 * <p>Remove it when the Airflow SDK no longer depends on kotlin-argparser.
 */
public class MissingValueException extends RuntimeException {

  public MissingValueException(String valueName) {
    super(valueName);
  }
}
