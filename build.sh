#!/bin/bash

mvn clean package

docker build -t liufuyang/kvrawclienttest .