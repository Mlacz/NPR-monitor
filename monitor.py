from mpi4py import MPI
import logging
import threading
import time

mpi_comm = MPI.COMM_WORLD
rank = mpi_comm.rank

class GetRank(logging.Filter):
    def filter(self, record):
        record.rank = rank
        record.seconds = time.strftime('%H:%M:%S', time.gmtime())
        return True

logging.basicConfig(
    format='%(seconds)s | %(rank)s | %(message)s')
log = logging.getLogger(__name__)
log.addFilter(GetRank())

class MonitorState:
    IDLE, \
    REQUESTING_CRITICAL, \
    IN_CRITICAL_SECTION, \
    WAITING \
    = range(4)

class MsgType:
    TOKEN_REQ, \
    TOKEN_LN, \
    TOKEN_Q, \
    VARIABLE, \
    NOTIFY \
    = range(5)

class Msg:
    def __init__(self, dictonary={}):
        self.variables = dictonary

    def __str__(self):
        return 'variables: {}' \
                .format(self.variables)

class Monitor:
    def __init__(self, id):
        self._id = id                           #id monitora
        self._notifylock = threading.Lock()     #zamek powiadomien zmiennych warunkowych 
        self._monitor_lock = threading.Lock()   #zamek sekcji krytycznej monitora
        self._convarlock = threading.Lock()     #zamek zmiennych warunkowych
        self._state = MonitorState.IDLE         
        self._wait_on = None                    

        #token 
        self._counter = 0
        self._token_present = False
        self._snlock = threading.Lock() 
        self._ln = []
        self._queue = []
        self._rn = []
        self._msg_tag = []

        #generowanie unikalnych (dla poszczegolnych monitorow) tagow do komunikacji 
        for i in range(5):
            self._msg_tag.append(self._gen_tag(i))
        
        #przygotowanie list ln i rn potrzebnych do generowania kolejki
        for i in range(mpi_comm.size):
            self._ln.append(0)

        for i in range(mpi_comm.size):
            self._rn.append(0)

        if (rank == 0 ):                    #token zaczyna zycie w procesie 0 
            self._token_present = True
        else:
            self._token_present = False

        #listener 
        self._listener = threading.Thread(target=self._listen)
        self._listener.start()
        self._listener2 = threading.Thread(target=self._listen_notify)
        self._listener2.start()

    def _gen_tag(self, tag):        
        self._temp = str(self._id)+str(tag)
        return int(self._temp)

    #zablokowanie monitora (rozpoczecie sekcji krytycznej)
    def _lock(self):
        if self._token_present == True:
            self._monitor_lock.acquire()
            self._state = MonitorState.IN_CRITICAL_SECTION
        else: 
            self._send_request()  #chce token! czekaj na token
            self._monitor_lock.acquire()
            self._state = MonitorState.IN_CRITICAL_SECTION

    #odblokowanie monitora (koniec sekcji krytycznej)
    def _unlock(self):
        self._monitor_lock.release()
       # time.sleep(0.3) # slow a little
        self._pass_token()
        self._state = MonitorState.IDLE

    #zwraca slownik zmiennych utworzonych przez uzytkownika 
    def _local_vars(self):
        return  dict([(attr, val) for attr, val in vars(self).iteritems() \
                if not (attr.startswith('__') or attr.startswith('_'))])

    def _request_handler(self, proces, sn):
        self._proces = proces
        self._snlock.acquire()
        if(self._rn[proces] < sn):
            self._rn[proces] = sn
        self._snlock.release()

    #proba oddania tokena
    def _pass_token(self):
        self._ln[rank]=self._rn[rank]
        if(self._token_present == False):
            return True
        else:
            for i in range(mpi_comm.size):
                if(i != rank and self._rn[i] == self._ln[i]+1 and i not in self._queue):
                    self._queue.append(i)
            if self._queue:
                send_id = self._queue.pop(0)
                self._send_token(send_id)
                return True
            else:
                return False

    def _send_token(self, id):
        self._token_present = False
        self._tag = self._msg_tag[MsgType.TOKEN_LN]
        mpi_comm.send(self._ln, dest=id, tag=self._tag)
        self._tag = self._msg_tag[MsgType.TOKEN_Q]
        mpi_comm.send(self._queue, dest=id, tag=self._tag)
        #send_data
        self._send_vars(id)

    def _send_vars(self, send_to):
        msg = Msg(self._local_vars())
        mpi_comm.send(msg, dest=send_to, tag=self._msg_tag[MsgType.VARIABLE])

    def _send_request(self):
        self._state = MonitorState.REQUESTING_CRITICAL
        self._counter += 1

        _temp = []
        _temp.append(self._id)
        _temp.append(self._counter)
        _temp.append(rank)
        for node in range(mpi_comm.size):
            if node != rank:
                mpi_comm.send(_temp, dest=node, tag=self._msg_tag[MsgType.TOKEN_REQ])

        self._recive_token()
        self._token_present = True
    
    def _recive_token(self):
        self._ln = mpi_comm.recv(tag=self._msg_tag[MsgType.TOKEN_LN])
        self._queue = mpi_comm.recv(tag=self._msg_tag[MsgType.TOKEN_Q])
        self._recive_vars()
        self._rn[rank] = self._counter 

    def _recive_vars(self):
        new_vars = mpi_comm.recv(tag=self._msg_tag[MsgType.VARIABLE])
        msg = new_vars.variables
        for attr, val in msg.iteritems():
            setattr(self, attr, val)

    def _listen_notify(self):
        _temp = []
        while(True):
            #listener starts here
            _temp = mpi_comm.recv(tag=self._msg_tag[MsgType.NOTIFY])
            self._notifylock.acquire()
            if(self._wait_on == _temp):
                self._convarlock.release()
                self._wait_on = None
            self._notifylock.release()

    def _listen(self):
        _temp = []
        while(True):
            #listener starts here
            _temp = mpi_comm.recv(tag=self._msg_tag[MsgType.TOKEN_REQ])
            self._request_handler(_temp[2], _temp[1])

class ConditionalVar: 
    def __init__(self, name, monitor):
        self._name = name
        self._monitor = monitor
    
    #zmienna warunkowa (signal)
    def signal(self):
        for node in range(mpi_comm.size):
            if node != rank:
                mpi_comm.send(self._name, dest=node, tag=self._monitor._msg_tag[MsgType.NOTIFY])

    #zmienna warunkowa (wait)
    def wait(self):
        self._monitor._notifylock.acquire()
        self._monitor._convarlock.acquire() 
        self._monitor._wait_on = self._name
        self._monitor._unlock()
        self._monitor._state = MonitorState.WAITING
        self._monitor._notifylock.release()

        while self._monitor._pass_token() == False:      #czekaj az bedzie komu oddac 
            time.sleep(0.1)

        self._monitor._convarlock.acquire()   #WAIT! 
        self._monitor._wait_on = None

        self._monitor._lock()
        self._monitor._state = MonitorState.IN_CRITICAL_SECTION
        self._monitor._convarlock.release()

# lock & unlock automation
def monitor_critical_section_method(f):
    def func_wrapper(monitor, *args, **kwargs):
        monitor._lock()
        meth = f(monitor, *args, **kwargs)
        monitor._unlock()
        return meth
    return func_wrapper
