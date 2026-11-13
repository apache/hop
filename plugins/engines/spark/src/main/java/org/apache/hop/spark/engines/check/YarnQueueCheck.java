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

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/** YARN: the queue exists and this user may submit to it. */
public class YarnQueueCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "YarnQueue";

  public YarnQueueCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.YARN;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(YarnResourceManagerCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    YarnClient client = context.get(YarnResourceManagerCheck.CLIENT);
    String queue = context.getSparkConfigs().getOrDefault("spark.yarn.queue", "default");
    QueueInfo info = client.getQueueInfo(queue);
    if (info == null) {
      List<String> queues = new ArrayList<>();
      for (QueueInfo q : client.getAllQueues()) {
        queues.add(q.getQueueName());
      }
      return error(
          "YARN has no queue '" + queue + "'. Its queues: " + String.join(", ", queues) + ".",
          "Set spark.yarn.queue=<queue> in the Spark config, with one of these queues.");
    }
    for (QueueUserACLInfo acl : client.getQueueAclsInfo()) {
      if (queue.equals(acl.getQueueName())
          || info.getQueuePath() != null && info.getQueuePath().equals(acl.getQueueName())) {
        if (!acl.getUserAcls().contains(QueueACL.SUBMIT_APPLICATIONS)
            && !acl.getUserAcls().contains(QueueACL.ADMINISTER_QUEUE)) {
          return error(
              "This user may not submit applications to the queue '" + queue + "'.",
              "Ask the cluster administrators for submit rights on the queue, or use another queue"
                  + " (spark.yarn.queue in the Spark config).");
        }
      }
    }
    return ok(
        "The queue '"
            + queue
            + "' is "
            + info.getQueueState()
            + " and takes applications of this user.");
  }
}
