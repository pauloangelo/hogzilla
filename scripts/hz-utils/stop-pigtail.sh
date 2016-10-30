#!/bin/bash


ps auxw | grep start-pigtail | grep -v grep | awk '{print $2}' | xargs kill -9
