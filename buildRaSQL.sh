#!/bin/sh

build/mvn -pl :spark-rasql_2.10 clean install
build/mvn -pl :spark-examples_2.10 clean package
