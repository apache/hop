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

You can provide values for the following variables in the values.yaml file:

## Variables hop server

Variable    | Required    | Description
---	|----	|---
server.enabled | Yes | Enable Hop server
server.replicaCount | Yes | Number of replicas
server.image.name | Yes | Hop server Image
server.image.tag | Yes | Hop release
server.image.env | No | Extra environnement variables, e.g. `HOP_SERVER_METADATA_FOLDER`
server.resources | No | Specify resources (CPU, Memory) available for the pod
server.service.type | Yes | Expose via Nodeport, Loadbalancer or ClusterIP
server.service.port | Yes | Port of the service
server.service.targetPort | Yes | Port of the pod(s)
server.service.name | Yes | Port name
server.hop.user | Yes | Username for hop-server
server.hop.port | Yes | The port for hop server
server.ingress.enabled | Yes | Enable ingress
server.ingress.className | No | Ingress class name
server.ingress.annotation | No | Arbitrary metadata
server.ingress.path | Yes | Path rule
server.ingress.pathType | Yes | Type of path, e.g. Prefix, ImplementationSpecific or Exact
server.ingress.hosts | Yes | List of hosts
server.nameOverride | No | Override Chart resource name
server.fullnameOverride | No | Override full resource name

## Variables hop web

Variable    | Required    | Description
---	|----	|---
web.enabled | Yes | Enable Hop web
web.replicaCount | Yes | Number of replicas
web.image.name | Yes | Hop web Image
web.image.tag | Yes | Hop web release
web.service.type | Yes | Expose via Nodeport, Loadbalancer or ClusterIP
web.service.port | Yes | Port of the service
web.service.targetPort | Yes | Port of the pod(s)
web.service.name | Yes | Port name
web.ingress.enabled | Yes | Enable ingress
web.ingress.className | No | Ingress class name
web.ingress.annotation | No | Arbitrary metadata
web.ingress.path | Yes | Path rule
web.ingress.pathType | Yes | Type of path, e.g. Prefix, ImplementationSpecific or Exact
web.ingress.hosts | Yes | List of hosts
web.nameOverride | No | Override Chart resource name 
web.fullnameOverride | No | Override full resource name

## How to run Hop via helm on kubernetes

### Prequest

Create a secret to store hop server credentials (give `<release_name>` a value of your choice)

```bash
kubectl create secret generic <release_name>-server --from-literal=pass=<admin_password>
```

`<admin_password>` will be used to authenticate to `hop-server` with admin account

### Install

Install the helm hop-server chart :

```bash
helm install <release_name> hop
```

`<release_name>` has to be the same given in the secret above