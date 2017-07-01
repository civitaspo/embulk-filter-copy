#!/bin/sh -x

PLUGIN_GITHUB_USER=civitaspo
PLUGIN_GROUP_ID=io.github.civitaspo
PLUGIN_ARTIFACT_ID=embulk-filter-copy
PLUGIN_VERSION=0.0.3.pre

mvn dependency:get \
     -Dmaven.repo.local=`cd&&pwd`/.embulk/m2/repository \
     -DremoteRepositories=https://raw.githubusercontent.com/${PLUGIN_GITHUB_USER}/${PLUGIN_ARTIFACT_ID}/gh-pages/ \
     -DgroupId=${PLUGIN_GROUP_ID} \
     -DartifactId=${PLUGIN_ARTIFACT_ID} \
     -Dversion=${PLUGIN_VERSION} \
     -Dtransitive=false
