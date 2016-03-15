import sys

sys.path.append('lib/esper/esper-5.3.0.jar')
sys.path.append('lib/esper/esper/lib/cglib-nodep-3.1.jar')
sys.path.append('lib/esper/esper/lib/commons-logging-1.1.3.jar')
sys.path.append('lib/esper/esper/lib/antlr-runtime-4.1.jar')


# import random
# import java.util.Map as Map
# import java.lang.Double
# import jython_utils
from com.espertech.esper.client import EPServiceProviderManager
from com.espertech.esper.client import Configuration
from com.espertech.esper.client import EPServiceProvider
from com.espertech.esper.client import UpdateListener
from com.espertech.esper.client import EPStatement
from com.espertech.esper.client.soda import StreamSelector

ISTREAM_ONLY = StreamSelector.ISTREAM_ONLY 
RSTREAM_ONLY = StreamSelector.RSTREAM_ONLY
RSTREAM_ISTREAM_BOTH = StreamSelector.RSTREAM_ISTREAM_BOTH    
    
class _EventListener(UpdateListener):
    def __init__(self, handler, stmtname):
        self.callback = handler
        self.stmtname = stmtname
    
    def update(self, *args):
        new_events = None
        if not args[0] is None:
            new_events = dict(args[0][0].getUnderlying())
        
        old_events = None
        if not args[1] is None:
            old_events = dict(args[1][0].getUnderlying())    
        
        self.callback(self.stmtname, new_events, old_events)

        
def EventListener(fnctn, stmtname):
    """
    Create a listener for CEP events that calls fncn(newEvents, oldEvents) per
    configuration.
    newEvents, oldEvents are standard Python dictionary classes
    """
    return _EventListener(fnctn, stmtname)
        

class EsperStatement(EPStatement):
    pass    
    
class EsperEngine():
    def __init__(self, engine_id):
        self.engine_id = engine_id
        
        config = Configuration()
        config.getEngineDefaults().getStreamSelection().\
            setDefaultStreamSelector(RSTREAM_ISTREAM_BOTH) 
        
        self._esperService = EPServiceProviderManager.\
            getProvider(self.engine_id, config)
 
        self._esperService.initialize()

    def define_event(self, eventtype, eventspec):
        self._esperService.getEPAdministrator().getConfiguration().\
            addEventType(eventtype, eventspec)

    def send_event(self, event, eventtype):
        self._esperService.getEPRuntime().sendEvent(event, eventtype)

    def create_query(self, stmt):
        return self._esperService.getEPAdministrator().createEPL(stmt) 



