#!/bin/bash


ps auxw | grep start-sflow2hz | grep -v grep | awk '{print $2}' | xargs kill -9
