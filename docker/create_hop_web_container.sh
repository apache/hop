#!/bin/bash

cd "${0%/*}"

unzip ../assemblies/web/target/hop.war -d ../assemblies/web/target/webapp
unzip ../assemblies/plugins/dist/target/hop-assemblies-*.zip -d ../assemblies/plugins/dist/target/

docker build ../ -f Dockerfile.web -t hop-web
