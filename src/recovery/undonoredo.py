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
    name = 'UndoNoRedoRecovery'

    def __init__(self, db):
        self.db = db

    def start_transaction(self, T):
        log = f'start, T{T.id}'
        self.db.att_cache_log(log)
        return log

    def RM_Read(self, T, data_item):
        log = f'read_item, T{T.id}, {data_item}, {self.db.data[data_item]}'
        self.db.att_cache_log(log)
        return log

    def RM_Write(self, T, data_item, new_value):
        if T not in self.db.active_transactions:
            self.db.add_active_transactions_list(T)

        old_value = self.db.data[data_item]
        self.db.data[data_item] = new_value
        log = f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}'
        self.db.att_cache_log(log)
        self.db.sync_cache_and_disk(T)
        return log
    
    def RM_Commit(self, T):
        log = f'commit, T{T.id}'
        self.db.att_cache_log(log)
        self.db.add_consolidated_transactions_list(T)
        self.db.remove_active_transactions_list(T)
        return log
    
    def RM_Abort(self, T):
        logs = []
        log = f'aborted, T{T.id}'
        logs.append(log)
        self.db.att_cache_log(log)
        self.db.att_disk_log(log)
        if ('start' in T.steps) & ('read_item' not in T.steps):
            data_item = T.data_item
            logs.append(self.RM_Read(T, data_item))
        if 'write_item' in T.steps:
            print(T.steps)
            filtered_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{T.id}']
            ImAn = filtered_log[0].split(', ')[-2]
            data_item = filtered_log[0].split(', ')[-3]
            logs.append(self.RM_Write(T, data_item, ImAn))
            self.db.add_aborted_transactions_list(T)
            self.db.remove_active_transactions_list(T)
        return logs
    
    def _undo(self, T):
        filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1]]
        print(filtered_logs)
        for l in reversed(filtered_logs):
            step = l.split(', ')[0]
            print(step)
            if step == 'write_item':
                old_value = l.split(', ')[-2]
                data_item = l.split(', ')[-3]
                log = self.RM_Write(T, data_item, old_value)
        return log

    def RM_Restart(self):
        logs = []
        not_consolidated = set(self.db.active_transactions).union(set(self.db.aborted_transactions)) - set(self.db.consolidated_transactions)
        not_consolidated = list(not_consolidated)
        print(not_consolidated)
        for T in not_consolidated:
            log = self._undo(T)
            logs.append(log)

        return logs
    