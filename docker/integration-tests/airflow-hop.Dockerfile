#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Airflow with a Java runtime and the Hop client of this build, for the airflow-dags integration
# tests: Airflow runs the workflows of a Hop project as DAGs through the Hop launcher.
FROM eclipse-temurin:21-jre AS jre

FROM apache/airflow:3.4.0
USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --chown=airflow:root ./assemblies/client/target/hop /opt/hop
USER airflow
ENV JAVA_HOME=/opt/java/openjdk

# Class data sharing: hop-conf and hop-run map the classes of an archive written now,
# instead of loading them at every start. Containers only read it (HOP_CDS=use).
# See lib/hop-cds.sh in the installation. The archives are part of the image, in a
# fixed folder: an audit folder set or mounted at run time doesn't hide them.
ENV HOP_CDS_FOLDER=/opt/hop/cds
RUN /opt/hop/lib/generate-cds-archives.sh
ENV HOP_CDS=use
