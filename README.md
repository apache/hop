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

# Apache Hop (incubating)

The Hop Orchestration Platform aims to facilitate all aspects of data and metadata orchestration.

[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=apache_incubator-hop&metric=ncloc)](https://sonarcloud.io/dashboard?id=apache_incubator-hop)
[![Jenkins Status](https://ci-builds.apache.org/buildStatus/icon?job=Hop%2FHop%2Fmaster)](https://ci-builds.apache.org/buildStatus/icon?job=Hop%2FHop%2Fmaster)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/apache/incubator-hop/graphs/commit-activity)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheHop?style=social)](https://twitter.com/ApacheHop)
[![Facebook](https://img.shields.io/badge/Facebook-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://www.facebook.com/projhop/)
[![Youtube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/channel/UCGlcYslwe03Y2zbZ1W6DAGA)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/company/hop-project/)

## Trying Apache Hop

Apache Hop aims to offer you a very stable user experience. Any feedback that you might have is welcome!  If you find
that something is not working, have an idea for a new feature or simply if something is not to your liking, let us know!
Your help is invaluable.

There are various things you can do to help Hop continue moving forward quickly:

- Log bugs, request features in the [Hop JIRA](https://issues.apache.org/jira/projects/HOP)
- Ask questions in the [mailing lists](https://hop.apache.org/community/mailing-list/)
  or [mattermost](https://chat.project-hop.org/hop/channels/dev)
- Help us write or fix documentation (lot of opportunities)
- Translate Hop. See our [i18n guide](https://hop.apache.org/dev-manual/latest/internationalisation.html).
- help fix some [sonar issues](https://sonarcloud.io/dashboard?id=apache_incubator-hop)

Check our [Contribution Guide](https://hop.apache.org/community/contributing/) and
the [Hop website](https://hop.apache.org) for more information on how to contribute.

## Building Apache Hop

### From source repository

Required:

- [OpenJDK](https://openjdk.java.net/) Java 8 compiler. Make sure to update your JDK to the latest possible patch
  version.

Recommended:

- [Maven](http://maven.apache.org/) 3.6.3 or higher

Clone Hop to a local repository:

    $ git clone https://github.com/apache/incubator-hop.git

Change into the clone repository and build: \
We provide two ways to build the code, if you have the correct Maven version installed you can use following commands:

    $ cd incubator-hop 
    $ mvn clean install 

We have also added mavenwrapper which simplifies build by using the correct Apache Maven version when it is not
available on the system

    $ cd incubator-hop
    $ ./mvnw clean install

### From release archive

You can download a source release [here](https://downloads.apache.org/incubator/hop/)

Required:

- [OpenJDK](https://openjdk.java.net/) Java 8 compiler or higher. Make sure to get the latest updates to support recent
  features like dark mode on the various platforms.
- [Maven](http://maven.apache.org/) 3.6.3 or higher

Unzip the archive

    $ tar -xf apache-hop*.tar.gz

Change into the clone repository and build: \
We provide two ways to build the code, if you have the correct Maven version installed you can use following commands:

    $ cd apache-hop-*-incubating
    $ mvn clean install

We have also added mavenwrapper which simplifies build by using the correct Apache Maven version when it is not
available on the system

    $ cd apache-hop-*-incubating
    $ ./mvnw clean install

## Run Apache Hop

After a successful build, the Hop UI can be started.

    $ cd assemblies/client/target
    $ unzip hop-client-*.zip
    $ cd hop 

On Windows, run `hop-gui.bat`, on Mac and Linux, run `hop-gui.sh`

Help us to improve Hop by logging issues in the [Hop JIRA](https://issues.apache.org/jira/projects/HOP)

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on
the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption
software, please check your country's laws, regulations and policies concerning the import, possession, or use, and
re-export of encryption software, to see if this is permitted. See http://www.wassenaar.org for more information.

The Apache Software Foundation has classified this software as Export Commodity Control Number (ECCN) 5D002, which
includes information security software using or performing cryptographic functions with asymmetric algorithms. The form
and manner of this Apache Software Foundation distribution makes it eligible for export under the "publicly available"
Section 742.15(b) exemption (see the BIS Export Administration Regulations, Section 742.15(b)) for both object code and
source code.

The following provides more details on the included cryptographic\
software:

* PGP Encrypt - Decrypt transforms/actions
* Apache Kafka Transforms
* Apache POI Transforms
* MQTT Transforms
