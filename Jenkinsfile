/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

def AGENT_LABEL = env.AGENT_LABEL ?: 'ubuntu'
def JDK_NAME = env.JDK_NAME ?: 'jdk_1.8_latest'
def MAVEN_NAME = env.MAVEN_NAME ?: 'maven_3_latest'

def MAVEN_PARAMS = "-T 2 -U -B -e -fae -V -Dmaven.compiler.fork=true -Dsurefire.rerunFailingTestsCount=2"

pipeline {

    agent {
        label AGENT_LABEL
    }

    tools {
        jdk JDK_NAME
        maven MAVEN_NAME
    }

    environment {
        MAVEN_SKIP_RC = true
        DOCKER_REPO='docker.io/apache/incubator-hop'
        DOCKER_REPO_WEB='docker.io/apache/incubator-hop-web'
    }

    options {
        buildDiscarder(
            logRotator(artifactNumToKeepStr: '5', numToKeepStr: '10')
        )
        disableConcurrentBuilds()
    }

    parameters {
        booleanParam(name: 'CLEAN', defaultValue: true, description: 'Perform the build in clean workspace')
    }

    stages {
        stage('Initialization') {
              steps {
                  echo 'Building Branch: ' + env.BRANCH_NAME
                  echo 'Using PATH = ' + env.PATH
              }
         }
         stage('Cleanup') {
              steps {
                  echo 'Cleaning up the workspace'
                  deleteDir()
              }
         }
        stage('Checkout') {
            steps {
                echo 'Checking out branch ' + env.BRANCH_NAME
                checkout scm
            }
        }
        stage ('Start Website build') {
            when {
                branch 'master'
                changeset 'docs/**'
            }
            steps {
                echo 'Trigger Documentation Build'
                build job: 'Hop/Hop-website/master', wait: false
            }
        }
        stage('Get POM Version') {
            when {
                  changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
                }
            steps{
                script {
                    env.POM_VERSION = sh script: 'mvn help:evaluate -Dexpression=project.version -q -DforceStdout', returnStdout: true
                }
                echo "The version fo the pom is: ${env.POM_VERSION}"
            }
        }
        stage('Test & Build') {
            when {
                 changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
                }
            steps {
                echo 'Test & Build'

                dir("local-snapshots-dir/") {
                    deleteDir()
                }

                sh "mvn $MAVEN_PARAMS -DaltDeploymentRepository=snapshot-repo::default::file:./local-snapshots-dir clean deploy"
            }
            post {
                always {
                    junit(testResults: '**/surefire-reports/*.xml', allowEmptyResults: true)
                    junit(testResults: '**/failsafe-reports/*.xml', allowEmptyResults: true)
                }
            }
        }
        stage('Unzip Apache Hop'){
            when {
                  changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
                }
            steps{
                sh "unzip ./assemblies/client/target/hop-client-*.zip -d ./assemblies/client/target/"
                sh "unzip ./assemblies/web/target/hop.war -d ./assemblies/web/target/webapp"
                sh "unzip ./assemblies/plugins/dist/target/hop-assemblies-*.zip -d ./assemblies/plugins/dist/target/"
            }
        }
        stage('Build Hop Docker Image') {
            when {
                branch 'master'
                changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
            }
            steps {
                echo 'Building Hop Docker Image'

                withDockerRegistry([ credentialsId: "dockerhub-hop", url: "" ]) {
                    //TODO We may never create final/latest version using CI/CD as we need to follow manual apache release process with signing
                    sh "docker build . -f docker/Dockerfile -t ${DOCKER_REPO}:${env.POM_VERSION}"
                    sh "docker push ${DOCKER_REPO}:${env.POM_VERSION}"
                    sh "docker rmi ${DOCKER_REPO}:${env.POM_VERSION}"
                  }
            }
        }
        stage('Build Hop Web Docker Image') {
            when {
                branch 'master'
                changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
            }
            steps {
                echo 'Building Hop Web Docker Image'

                withDockerRegistry([ credentialsId: "dockerhub-hop", url: "" ]) {
                    //TODO We may never create final/latest version using CI/CD as we need to follow manual apache release process with signing
                    sh "docker buildx create --name hop --use"
                    sh "docker buildx build --platform linux/amd64,linux/arm64 . -f docker/Dockerfile.web -t ${DOCKER_REPO_WEB}:${env.POM_VERSION} --push"
                    sh "docker buildx rm hop"
                  }
            }
        }
        stage('Deploy'){
            when {
                branch 'master'
                changeset pattern: "^(?!docs).*^(?!integration-tests).*" , comparator: "REGEXP"
            }
            steps{
                echo 'Deploying'
                sh 'mvn -X -P deploy-snapshots wagon:upload'
            }
        }

    }
    post {
        always {
            cleanWs()
            emailext(
                subject: '${DEFAULT_SUBJECT}',
                body: '${DEFAULT_CONTENT}',
                recipientProviders: [[$class: 'CulpritsRecipientProvider']]
            )
        }
    }
}
