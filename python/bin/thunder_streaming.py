#!/bin/python

import os
import sys
import ts2

HBASE_VERSION = '0.98.7-hadoop1'
JAR_ROOT = '/home/andrew/.ivy2/cache/org.apache.hbase/'

def getSparkHome():
    sparkhome = os.getenv("SPARK_HOME")
    if sparkhome is None:
        raise Exception("The environment variable SPARK_HOME must be set to the Spark installation directory")
    if not os.path.exists(sparkhome):
        raise Exception("No Spark installation at %s, check that SPARK_HOME is correct" % sparkhome)
    return sparkhome

def findThunderStreamingJar():
    thunderStreamingDir = os.path.dirname(os.path.realpath(ts2.__file__))
    thunderStreamingJar = os.path.join(thunderStreamingDir, 'lib', 'thunder_streaming-assembly-'+str(ts2.__version__)+'.jar')
    if not os.path.isfile(thunderStreamingJar):
        raise Exception("Thunder Streaming jar file not found at '%s'. Does Thunder need to be rebuilt?")
    return [thunderStreamingJar]

def findHBaseJars(): 
    """
    For now, assume that all the HBase jars are in the Ivy cache (meaning thunder_streaming was built on this 
    machine).
    """
    names = ['hbase', 'hbase-common', 'hbase-protocol', 'hbase-server', 'hbase-client', 'hbase-annotations', 'hbase-prefix-tree']
    version = HBASE_VERSION
    return [os.path.join(JAR_ROOT, name, 'jars', name + '-' + version + '.jar') for name in names]

def findOtherJars(): 
    return ['/home/andrew/.ivy2/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.5.0.jar',
            '/home/andrew/.ivy2/cache/org.cloudera.htrace/htrace-core/jars/htrace-core-2.04.jar',
    ]

def main():
    SPARK_HOME = getSparkHome()

    sparkSubmit = os.path.join(SPARK_HOME, 'bin', 'pyspark')

    # add python script
    os.environ['PYTHONSTARTUP'] = os.path.join(os.path.dirname(os.path.realpath(ts2.__file__)), 'util', 'launch.py')

    # add ETL configuration
    os.environ['ETL_CONFIG'] = sys.argv[1]

    thunderStreamingJar = findThunderStreamingJar()
    #hbaseJars = findHBaseJars()
    #otherJars = findOtherJars()
	
    #jarList = thunderStreamingJar + hbaseJars + otherJars
    jarList = thunderStreamingJar

    jars = ['--jars', ','.join(jarList)]
    driver_classpath = ['--driver-class-path', ':'.join(jarList)]

    retvals = []
    retvals.extend(jars)
    retvals.extend(driver_classpath)

    print "retvals: %s" % str(retvals) 

    os.execv(sparkSubmit, retvals)

if __name__ == "__main__":
    main()
