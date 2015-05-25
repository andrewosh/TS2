import os
import sys

import ts2

def getSparkHome():
    sparkhome = os.getenv("SPARK_HOME")
    if sparkhome is None:
        raise Exception("The environment variable SPARK_HOME must be set to the Spark installation directory")
    if not os.path.exists(sparkhome):
        raise Exception("No Spark installation at %s, check that SPARK_HOME is correct" % sparkhome)
    return sparkhome

def main():
    SPARK_HOME = getSparkHome()

    childArgs = transformArguments(sys.argv)

    sparkSubmit = os.path.join(SPARK_HOME, 'bin', 'pyspark')

    # add python script
    os.environ['PYTHONSTARTUP'] = os.path.join(os.path.dirname(os.path.realpath(ts2.__file__)), 'utils', 'launch.py')

    # add ETL configuration
    os.environ['ETL_CONFIG'] = sys.argv[1]

    # check for ipython option
    if "-i" in childArgs:
        os.environ['IPYTHON'] = '1'

    # check for notebook option
    if "-n" in childArgs:
        os.environ['IPYTHON_OPTS'] = 'notebook'

    os.execv(sparkSubmit, childArgs)

if __name__ == "__main__":
    main()