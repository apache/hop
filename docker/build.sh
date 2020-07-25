#!/bin/sh

cp ../assemblies/web/target/hop.war ./
cp ../assemblies/plugins/dist/target/hop-assemblies-plugins-dist-*.zip ./hop-assemblies-plugins-dist.zip
docker build -t project-hop .
