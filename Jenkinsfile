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
        stage ('Start Docs build') {
            when {
                branch 'master'
                changeset '**/*.adoc'
            }
            steps {
                echo 'Trigger Documentation Build'
                build job: 'Hop/Hop-Documentation/asf-site', wait: false
            }
        }
        stage('Test & Build') {
            when {
                branch 'master'
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
        stage('Code Quality') {
            steps {
                echo 'Checking Code Quality on SonarCloud'
                withCredentials([string(credentialsId: 'sonarcloud-key-apache-hop', variable: 'SONAR_TOKEN')]) {
                    sh 'mvn sonar:sonar -Dsonar.host.url=https://sonarcloud.io -Dsonar.organization=apache -Dsonar.projectKey=apache_incubator-hop -Dsonar.branch.name=${BRANCH_NAME} -Dsonar.login=${SONAR_TOKEN}'
                }
            }
        }
        stage('Deploy'){
            when {
                branch 'master'
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
