#!/usr/bin/env bash

export DATA_DIR=./data/

mvn clean package
java -cp ./libs/*:./target/de-case-1.0-SNAPSHOT.jar com.dataengineering.ClickStreamAnalysis case.csv