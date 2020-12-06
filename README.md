# Hop

The Hop Orchestration Platform aims to facilitate all aspects of data and metadata orchestration.

[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=apache_incubator-hop&metric=ncloc)](https://sonarcloud.io/dashboard?id=apache_incubator-hop) 
[![Jenkins Status](https://ci-builds.apache.org/buildStatus/icon?job=Hop%2FHop%2Fmaster)](https://ci-builds.apache.org/buildStatus/icon?job=Hop%2FHop%2Fmaster)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/apache/incubator-hop/graphs/commit-activity)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheHop?style=social)](https://twitter.com/ApacheHop)
[![Facebook](https://img.shields.io/badge/Facebook-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://www.facebook.com/projhop/)
[![Youtube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/channel/UCGlcYslwe03Y2zbZ1W6DAGA)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/company/hop-project/)


## Trying Hop 

Hop is work in progress. After building, the UI can be started, but a lot of things may be broken. 

There are various things you can do to help Hop moving forward as fast as possible: 

- log bugs, request features in the [Hop JIRA](https://issues.apache.org/jira/projects/HOP)
- asking questions to the mailing list or [mattermost](https://chat.project-hop.org/hop/channels/dev)
- help with documentation (lot of opportunities)
- help fixing some [sonar issues](https://sonarcloud.io/dashboard?id=apache_incubator-hop)

Check our [Contribution Guide](http://www.project-hop.org/community/contributing/) and the [Hop website](http://www.project-hop.org/) for more information on how to contribute.  

## Build Hop 


Required: 
- [OpenJDK](https://openjdk.java.net/) Java 8 compiler 
- [Maven](http://maven.apache.org/)

Clone Hop to a local repository: 

    $ git clone https://github.com/apache/incubator-hop.git

Change into the clone repository and build: 

    $ cd incubator-hop 
    $ mvn clean install 

## Run Hop 

After a successful build, the Hop UI can be started.

    $ cd assemblies/client/target
    $ unzip hop-assemblies-client-*.zip
    $ cd hop 

On Windows, run `hop-ui.bat`, on Mac and Linux, run `hop-ui.sh` 

Help us to improve Hop by logging issues in the [Hop JIRA](https://issues.apache.org/jira/projects/HOP)
