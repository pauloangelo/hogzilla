#!/bin/bash

grep -r HogConfig.get ../src | sed 's/.*HogConfig.get.*(config[ ]*,[ ]*"\([a-Z0-9.]*\)",\([ a-Z0-9,"()]*\)).*/\1 = \2/' | sort | sed 's/\"//g' | sed 's/\(.*\)\.\(.*\) = \(.*\)/\1 {\n\t\t\2 = "\3"\n\t}/g'
