# The classes in this script observe new file createion events in target directory
# and then invoke corresponding functions.

import time
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler


# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
# CLASS: Watcher
# DESCRIPTION: This class keep monitoring the target path
#              If new file is created, it calls Handler function
# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
class Watcher:

    def __init__(self):
        self.observer = Observer()
        self.observers = []


    # --*--*--*--*--*--*--*--*--
    # Function: Add_target
    # Description: Add directory to monitor and corresponding data handler class
    #  @ Input: 
    #  @ Output: 
    # --*--*--*--*--*--*--*--*--
    def Add_target(self, directory_to_watch, class_to_call):
        handler = Handler(class_to_call)
        self.observer.schedule(handler, directory_to_watch, recursive=True)
        self.observers.append(self.observer)


    # --*--*--*--*--*--*--*--*--
    # Function: Run
    # Description: Start observation for the target directory
    #  @ Input: 
    #  @ Output: 
    # --*--*--*--*--*--*--*--*--
    def Run(self):
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            for o in self.observers:
                o.unschedule_all()
                o.stop()
            print("Event handler fails")
            print "Unexpected error:", sys.exc_info()[0]
        for o in self.observers:
            o.join()




# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
# CLASS: Handler
# DESCRIPTION: It calls callback function as given by caller
# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
class Handler(LoggingEventHandler):
    
    def __init__(self, class_to_call):
        self.data_handler = class_to_call


    # --*--*--*--*--*--*--*--*--
    # Function: on_created
    # Description: call data handler on any event
    #  @ Input: event
    #  @ Output: 
    # --*--*--*--*--*--*--*--*--
    def on_created(self, event):
        self.data_handler.Do_all()        
                



#--8<--8<--8<--8<--8<--8<--8<--8<- TESTING ONLY -8<--8<--8<--8<--8<--8<--8<--8<--8<--

from data_handler import Processed_data_handler
if __name__ == '__main__':

    DIR1 = 'dir1_processed_data'
    DIR2 = 'dir2_regular_raw_data'
    REPO1 = '.'
    REPO2 = '.'
    dummy_handler = Processed_data_handler(DIR1,REPO1)
    dummy_handler2 = Processed_data_handler(DIR2,REPO2)
    
    w = Watcher()
    w.Add_target(DIR1, dummy_handler)
    w.Add_target(DIR2, dummy_handler2)
    w.Run()
    


