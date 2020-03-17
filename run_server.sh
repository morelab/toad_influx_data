#!/bin/sh

ORIGIN_DIRECTORTY=$(pwd)
BASEDIR=$(dirname "$0")
{
  cd "$BASEDIR"
  # run main
} ||{
  :
}
cd "$ORIGIN_DIRECTORTY"