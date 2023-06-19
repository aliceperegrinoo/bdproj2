## Opção mais complexa
## Mais flexível sobre quando transferir dados da cache para o disco, deixando a decisão para o
### gerenciador da cache
## Evita transferências desnecessárias, minimizando E/S
## Minimiza custo de armazenamento na cache
## Maximiza desempenho em condições normais de processamento
## Maior custo de recuperação

## UNDO é necessário => Se o Gerenciador de Cache salva x no disco e Ti aborta ou uma falha ocorre 
### antes de Ti ser efetivada
## REDO é necessário => Se Ti for efetivada e a falha ocorrer antes do Gerenciador de Cache
### liberar a partição de x

class UndoNoRedoRecovery:
    def __init__(self, db):
        self.db = db

    def RM_Read(self, T, data_item):
        self.db.att_cache_log(f'read_item, T{T.id}, {data_item}, {self.db.data[data_item]}')
        self.db.att_disk_log(f'read_item, T{T.id}, {data_item}, {self.db.data[data_item]}')

    def RM_Write(self, T, data_item, new_value):
        self.db.add_active_transactions_list(T)
        self.RM_Read(T, data_item)
        old_value = self.db.data[data_item]
        self.db.data[data_item] = new_value
        self.db.att_cache_log(f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}')
        self.db.att_disk_log(f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}')
    
    def RM_Commit(self, T):
        self.db.att_cache_log(f'commit, T{T.id}')
        self.db.att_disk_log(f'commit, T{T.id}')
        self.db.add_consolidated_transactions_list(T)
        self.db.remove_active_transactions_list(T)
    
    def RM_Abort(self, T):
        if 'write_item' in T.steps:
            filtered_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{T.id}']
            ImAn = filtered_log[0].split(', ')[-2]
            data_item = filtered_log[0].split(', ')[-3]
            self.RM_Write(T, data_item, ImAn)
        self.db.add_aborted_transactions_list(T)
        self.db.remove_active_transactions_list(T)
    
    def _undo(self, T):
        filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1]]
        for log in reversed(filtered_logs):
            step = log.split(', ')[0]
            if step == 'write_item':
                old_value = log.split(', ')[-2]
                data_item = log.split(', ')[-3]
                self.RM_Write(T, data_item, old_value)

    def RM_Restart(self):
        not_consolidated = set(self.db.active_transactions).union(set(self.db.aborted_transactions)) - set(self.db.consolidated_transactions)
        not_consolidated = list(not_consolidated)
        for T in not_consolidated:
            self._undo(T)

    