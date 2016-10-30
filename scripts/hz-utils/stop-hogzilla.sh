#!/bin/bash

ps auxw | grep start-hogzilla | grep -v grep | awk '{print $2}' | xargs kill -9
