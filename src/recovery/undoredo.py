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

class UndoRedoRecovery:
    name = 'UndoRedoRecovery'

    def __init__(self, db):
        self.db = db

    def start_transaction(self, T):
        log = f'start, T{T.id}'
        self.db.att_cache_log(log)
        T.steps.append('start')
        return log
    
    def terminate_transaction(self, T):
        log = f'end, T{T.id}'
        self.db.att_cache_log(log)
        T.steps.append('end')
        return log

    def RM_Read(self, T, data_item):
        log = f'read_item, T{T.id}, {data_item}, {self.db.data[data_item]}'
        self.db.att_cache_log(log)
        T.steps.append('read_item')
        return log

    def RM_Write(self, T, data_item, new_value):
        if T not in self.db.active_transactions:
            self.db.add_active_transactions_list(T)

        T.steps.append('write_item')
        old_value = self.db.data[data_item]
        self.db.data[data_item] = new_value
        log = f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}'
        self.db.att_cache_log(log)
        return log
    
    def RM_Commit(self, T, type='default'):
        if type == 'default':
            logs = []
            logs_to_sync = self.db.sync_cache_and_disk(T)
            logs.extend(logs_to_sync)
            commit_log = f'commit, T{T.id}'
            logs.append(commit_log)
            for l in logs:
                self.db.att_disk_log(l)
            self.db.add_consolidated_transactions_list(T)
            self.db.remove_active_transactions_list(T)
            T.steps.append('commit')
            return logs
        if type == 'simplified':
            log = f'commit, T{T.id}'
            self.db.att_disk_log(log)
            self.db.add_consolidated_transactions_list(T)
            self.db.remove_active_transactions_list(T)
            T.steps.append('commit')
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
            filtered_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{T.id}']
            ImAn = filtered_log[0].split(', ')[-2]
            data_item = filtered_log[0].split(', ')[-3]
            logs.append(self.RM_Write(T, data_item, ImAn))
            self.db.add_aborted_transactions_list(T)
            self.db.remove_active_transactions_list(T)
        return logs
    
    def _redo(self, T):
        filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1]]
        for log in filtered_logs:
            step = log.split(', ')[0]
            if step == 'write_item':
                new_value = log.split(', ')[-1]
                data_item = log.split(', ')[-3]
                self.RM_Write(T, data_item, new_value)
    
    def _undo(self, T):
        filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1]]
        for log in reversed(filtered_logs):
            step = log.split(', ')[0]
            if step == 'write_item':
                old_value = log.split(', ')[-2]
                data_item = log.split(', ')[-3]
                self.RM_Write(T, data_item, old_value)

    def RM_Restart(self):
        for T in self.db.aborted_transactions:
            self._undo(T)
        for T in self.db.active_transactions:
            self._undo(T)
        for T in self.db.consolidated_transactions:
            self._redo(T)


