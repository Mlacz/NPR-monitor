from monitor import *
import time
import logging
import random

random.seed(rank * time.time())

BUFFER_SIZE = 3
ITEMS = ['Door', 'Bed', 'Pen', 'Monitor', 'Foot', 'Laptop', 'Linux', 'Test', 'Cars']
COLORS = ['RED', 'PINK', 'WHITE', 'BLACK', 'GREEN', 'BROWN', 'YELLOW', 'GRAY']

class ProducerConsumerMonitor(Monitor):
    def __init__(self, id):
        Monitor.__init__(self, id)
        self.item_count = 0
        self.item_buffer = []   
        self._cv_full = ConditionalVar('full', self)    #zmienna warunkowa 1
        self._cv_empty = ConditionalVar('empty', self)  #zmienna warunkowa 2
    
    
    @monitor_critical_section_method
    def add(self, item):
        while self.item_count == BUFFER_SIZE:
            self._cv_full.wait()
        self.item_buffer.append(item)
        self.item_count += 1
        if self.item_count == 1:
            self._cv_empty.signal()

    @monitor_critical_section_method
    def remove(self):
        while self.item_count == 0:
            self._cv_empty.wait()
        item = self.item_buffer.pop(0)
        self.item_count -= 1
        if self.item_count == (BUFFER_SIZE-1):
            self._cv_full.signal()
        return item
        


if __name__ == '__main__':
    m = ProducerConsumerMonitor(8)
    
    while True:
        if rank%2 == 0:
            item = '{} {}'.format(random.choice(COLORS), random.choice(ITEMS))
            m.add(item)
            log.warning('%s\t --> %s', item, m.item_buffer)
            time.sleep(random.choice([0.3,1]))
        else:
            item = m.remove()
            log.warning('%s\t <-- %s', item, m.item_buffer)
            time.sleep(random.choice([0.3,1]))
