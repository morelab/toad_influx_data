#!/bin/sh

ORIGIN_DIRECTORTY=$(pwd)
BASEDIR=$(dirname "$0")
{
  cd "$BASEDIR"
  python -m toad_influx_data.main
} ||{
  :
}
cd "$ORIGIN_DIRECTORTY"