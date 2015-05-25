# File monitoring parameters
MOD_TIME = 0.25

# HBase connection parameters
HBASE_HOST = 'localhost'

# HBase schema information
HBASE_TABLE = 'data'
BASE_COL_FAM = 'base'
BASE_COL_QUALIFIER = 'dataset'
DERIVATIVE_COL_FAM = 'derivative'
HBASE_FAMILIES = {BASE_COL_FAM: dict(), DERIVATIVE_COL_FAM: dict()}
MAX_KEY = 1000000000000

# Logging-related
LOG_FILE = "feeder.log"
