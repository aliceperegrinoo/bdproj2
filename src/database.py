"""
| item de dado | valor |
|       x      |   5   |
|       y      |   2   |

Modelo transação = [Ti, X, operacao, valor_anterior, valor_atual]
"""

from .recovery.undoredo import UndoRedoRecovery
from .recovery.undonoredo import UndoNoRedoRecovery

class Database:
    def __init__(self, data):
        self.data = data
        self.cache_log = []
        self.disk_log = []
        self.active_transactions = []
        self.consolidated_transactions = []
        self.aborted_transactions = []
    
    def get_checkpoint(self, *T):
        ids = ['T'+tr.id for tr in T]
        self.att_cache_log(f'checkpoint, {ids}')
        self.sync_cache_and_disk_on_checkpoint()

    def sync_cache_and_disk_on_checkpoint(self):
        add_to_disk = []
        for cache in self.cache_log:
            if cache not in self.disk_log:
                add_to_disk.append(cache)
        return add_to_disk

    def sync_cache_and_disk(self, T):
        print(T.steps)
        if T.steps[-1] == 'end':
            filtered_logs = [log for log in self.cache_log if log.split(', ')[1] == f'T{T.id}']  
            self.disk_log.extend(filtered_logs)

        return filtered_logs

    def att_cache_log(self, status):
        self.cache_log.append(status)

    def clean_cache_log(self):
        self.cache_log = []

    def att_disk_log(self, status):
        self.disk_log.append(status)

    def add_active_transactions_list(self, T):
        self.active_transactions.append(T)

    def remove_active_transactions_list(self, T):
        self.active_transactions.remove(T)
    
    def add_consolidated_transactions_list(self, T):
        self.consolidated_transactions.append(T)

    def remove_consolidated_transactions_list(self, T):
        self.consolidated_transactions.remove(T)
    
    def add_aborted_transactions_list(self, T):
        self.aborted_transactions.append(T)

    def remove_aborted_transactions_list(self, T):
        self.aborted_transactions.remove(T)
    
    def failure(self, recovery_mode):
        self.clean_cache_log()
        if recovery_mode == 'UNDOREDO':
            recovery = UndoRedoRecovery(self)
            recovery.RM_Restart()
        elif recovery_mode == 'UNDONOREDO':
            recovery = UndoNoRedoRecovery(self)
            recovery.RM_Restart()
    