<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# hop-helm

A **Hop Helm chart** supporting **long-lived** setups.

## Variables

You can provide values for the following variables in the values.yaml file:

Variable    | Required    | Description
---	|----	|---
replicaCount | Yes | Number of replicas
image.tag | Yes | Hop release
service.type | Yes | Expose via Nodeport or Loadbalancer
service.port | Yes | Port of the service
service.targetPort | Yes | Port of the pod(s)
hop.user | Yes | Username for hop-server
hop.port | Yes | The port for hop server

## How to run Hop server via helm on kubernetes

Create a secret for the hop server

```bash
kubectl create secret generic hop-server   --from-literal=pass=admin
```

install the helm hop-server chart:

```bash
helm install  hop-server hop-server/
```