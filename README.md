# Hop

The Hop Orchestration Platform aims to facilitate all aspects of data and metadata orchestration.

## Trying Hop 

Hop is work in progress. After building, the UI can be started, but a lot of things will be broken. 

There are various things you can do to help Hop moving forward as fast as possible: 

- log bugs, request features in the [Hop JIRA](https://project-hop.atlassian.net)
- answer questions in the [Hop forums](https://forums.project-hop.org/)
- help with documentation 
- spread the word

Check our [Contribution Guide](http://www.project-hop.org/community/contributing/) and the [Hop website](http://www.project-hop.org/) for more information on how to contribute.  

## Build Hop 


Required: 
- [OpenJDK](https://openjdk.java.net/) Java 8 compiler 
- [Maven](http://maven.apache.org/)
- [RAP](https://github.com/HiromuHota/pentaho-kettle#rap)

Clone Hop to a local repository: 

    $ git clone https://github.com/project-hop/hop.git

Change into the clone repository and build: 

    $ cd hop 
    $ mvn clean install
    $ cd docker
    $ sh ./build.sh

## Run Hop 

After a successful build, the Hop UI can be started.

    $ docker run -d -p 8080:8080 --name hop project-hop

Help us to improve Hop by logging issues in the [Hop JIRA](https://project-hop.atlassian.net)
