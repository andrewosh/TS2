"""
Start Thunder component
"""

import thunder

from thunder.utils.context import ThunderContext

try:
    from termcolor import colored
except ImportError:
    colored = lambda x, y: x

tsc = ThunderContext(sc)

print('')
print(colored('       IIIII            ', 'yellow'))
print(colored('       IIIII            ', 'yellow'))
print(colored('    IIIIIIIIIIIIIIIIIII ', 'yellow'))
print(colored('    IIIIIIIIIIIIIIIIII  ', 'yellow'))
print(colored('      IIIII             ', 'yellow'))
print(colored('     IIIII              ', 'yellow'))
print(colored('     IIIII              ', 'yellow') + 'Thunder')
print(colored('      IIIIIIIII         ', 'yellow') + 'version ' + thunder.__version__)
print(colored('       IIIIIII          ', 'yellow'))
print('')

print "+"

"""
 Start streaming component
"""

from ts2.etl.feeder import Feeder
from ts2.util.context import ThunderStreamingContext
from ts2.db.manager import HBaseManager
from ts2 import __version__
import os

etl_conf = os.environ['ETL_CONFIG']
db_manager = HBaseManager()
f = Feeder(etl_conf, db_manager)

tssc = ThunderStreamingContext(tsc, sc)

print('')
print(colored('       IIIII                       IIIIIII    ', 'red'))
print(colored('       IIIII                  IIIIIIIIIII     ', 'red'))
print(colored('    IIIIIIIIIIIIIIIIIII   IIIIIIIIII          ', 'red'))
print(colored('    IIIIIIIIIIIIIIIIII   IIIIIIIIIII          ', 'red'))
print(colored('      IIIII               IIIIIIIIIII         ', 'red'))
print(colored('     IIIII                  IIIIIIIIIIII      ', 'red'))
print(colored('     IIIII                  IIIIIIIIIII       ', 'red') + 'Thunder Streaming')
print(colored('      IIIIIIIII            IIIIIIIIII         ', 'red') + 'version ' + ts2.__version__)
print(colored('       IIIIIII          IIIIIIIII             ', 'red'))
print('')


print('A Thunder context is available as tsc')
print('A ThunderStreaming context is available as tssc')

