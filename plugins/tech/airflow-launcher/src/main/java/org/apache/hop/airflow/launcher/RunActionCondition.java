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

package org.apache.hop.airflow.launcher;

import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.ConditionTask;
import org.apache.airflow.sdk.Context;

/**
 * The task of an action with both success and failure hops leaving it: it runs the action, and its
 * result decides which side runs next. Airflow skips the other side. The task fails only when Hop
 * could not run the action at all.
 */
public class RunActionCondition implements ConditionTask {

  @Override
  public boolean decide(Context context, Client client) throws Exception {
    Object exitCode = RunWorkflowTask.run(context, client).get("exitCode");
    return exitCode instanceof Number number && number.intValue() == 0;
  }
}
