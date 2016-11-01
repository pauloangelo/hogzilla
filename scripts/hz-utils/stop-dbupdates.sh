#!/bin/bash

ps auxw | grep start-dbupdates.sh | grep -v grep | awk '{print $2}' | xargs kill -9
