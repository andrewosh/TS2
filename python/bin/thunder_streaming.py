#!/bin/python

import os
import sys
import ts2

def getCommaSeparatedOptionsList(childOptionsFlag, commaSeparated, additionalDefault=None):
    lst = commaSeparated.split(",") if commaSeparated else []
    if additionalDefault:
        lst.append(additionalDefault)
    if lst:
        return [childOptionsFlag, ",".join(lst)]
    return []

def getSparkHome():
    sparkhome = os.getenv("SPARK_HOME")
    if sparkhome is None:
        raise Exception("The environment variable SPARK_HOME must be set to the Spark installation directory")
    if not os.path.exists(sparkhome):
        raise Exception("No Spark installation at %s, check that SPARK_HOME is correct" % sparkhome)
    return sparkhome

def findThunderStreamingJar():
    thunderStreamingDir = os.path.dirname(os.path.realpath(ts2.__file__))
    thunderStreamingJar = os.path.join(thunderStreamingDir, 'lib', 'thunder_streaming_2.10-'+str(ts2.__version__)+'.jar')
    if not os.path.isfile(thunderStreamingJar):
        raise Exception("Thunder Streaming jar file not found at '%s'. Does Thunder need to be rebuilt?")
    return thunderStreamingJar

def main():
    SPARK_HOME = getSparkHome()

    sparkSubmit = os.path.join(SPARK_HOME, 'bin', 'pyspark')

    # add python script
    os.environ['PYTHONSTARTUP'] = os.path.join(os.path.dirname(os.path.realpath(ts2.__file__)), 'util', 'launch.py')

    # add ETL configuration
    os.environ['ETL_CONFIG'] = sys.argv[1]

    thunderStreamingJar = findThunderStreamingJar()
    jars = getCommaSeparatedOptionsList("--jars", opts.get("jars", []), thunderStreamingJar)

    retvals = []
    retvals.extend(jars)

    os.execv(sparkSubmit, retvals)

if __name__ == "__main__":
    main()