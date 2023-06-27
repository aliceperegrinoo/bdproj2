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

    def RM_Read(self, T, data_item, db):
        log = f'read_item, T{T.id}, {data_item}, {db[data_item]}'
        self.db.att_cache_log(log)
        T.steps.append('read_item')
        return log

    def RM_Write(self, T, data_item, new_value, att_cache=True):
        if T not in self.db.active_transactions:
            self.db.add_active_transactions_list(T)

        # Puxa todos os logs do tipo "read_item" referentes a um "data_item" específico
        if T.steps[-1] == 'read_item':
            if len(self.db.cache_log) > 0: 
                read_logs = [log for log in self.db.cache_log if \
                                 log.split(', ')[0] == 'read_item' \
                                    and log.split(', ')[2] == data_item]
            else:
                read_logs = [log for log in self.db.disk_log if \
                             log.split(', ')[0] == 'read_item' \
                                and log.split(', ')[2] == data_item]
            
            old_value = read_logs[-1].split(', ')[-1]

        # Puxa todos os logs do tipo "write_item" referentes a um "data_item" específico
        elif T.steps[-1] == 'write_item':
            if len(self.db.cache_log) > 0: 
                write_logs = [log for log in self.db.cache_log if  \
                                 log.split(', ')[0] == 'write_item' and \
                                    log.split(', ')[2] == data_item]
            else:
                write_logs = [log for log in self.db.disk_log if \
                             log.split(', ')[0] == 'write_item' \
                                and log.split(', ')[2] == data_item]

            last_write_log = write_logs[-1]
            old_value = last_write_log.split(', ')[-1]

        else: 
            if len(self.db.cache_log) > 0: 
                write_logs = [log for log in self.db.cache_log if  \
                                log.split(', ')[0] == 'write_item' and \
                                    log.split(', ')[2] == data_item]
            else:
                write_logs = [log for log in self.db.disk_log if \
                            log.split(', ')[0] == 'write_item' \
                                and log.split(', ')[2] == data_item]

            last_write_log = write_logs[-1]
            old_value = last_write_log.split(', ')[-1]

        T.steps.append('write_item')
        log = f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}'
        if att_cache == True:
            self.db.att_cache_log(log)
        return log
    
    def RM_Commit(self, T, type='default'):
        if type == 'default':
            logs = []
            logs_to_sync = self.db.sync_cache_and_disk(T)
            logs_to_sync = self.db.check_for_duplicates_disk_log(logs_to_sync)
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
        T.steps.append('abort')
        logs.append(log)
        self.db.att_cache_log(log)
        # self.db.att_disk_log(log)
        # if ('start' in T.steps) & ('read_item' not in T.steps):
        #     data_item = T.data_item
        #     logs.append(self.RM_Read(T, data_item))
        # if 'write_item' in T.steps:
        #     filtered_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{T.id}']
        #     ImAn = filtered_log[0].split(', ')[-2]
        #     data_item = filtered_log[0].split(', ')[-3]
        #     logs.append(self.RM_Write(T, data_item, ImAn))
        #     self.db.add_aborted_transactions_list(T)
        #     self.db.remove_active_transactions_list(T)
        return logs
    
    def _redo(self, T):
        redo_updates = []
        for di in T.data_item:
            filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1] \
                                and not log.startswith('start') \
                                    and not log.startswith('start') \
                                        and not log.startswith('abort') \
                                            and not log.startswith('end') \
                                                and not log.startswith('commit') \
                                                    and log.split(', ')[2] == di]
            for log in filtered_logs:
                step = log.split(', ')[0]
                if step == 'write_item':
                    new_value = log.split(', ')[-1]
                    data_item = log.split(', ')[-3]
                    self.RM_Write(T, data_item, new_value, att_cache=False)
                    redo_updates.append((data_item, new_value))

        return redo_updates
    
    def _undo(self, T):
        undo_updates = []
        for di in T.data_item:
            filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1] \
                              and not log.startswith('start') \
                                and not log.startswith('abort') \
                                     and not log.startswith('end') \
                                          and not log.startswith('commit') \
                                            and log.split(', ')[2] == di]
            for log in reversed(filtered_logs):
                step = log.split(', ')[0]
                if step == 'write_item':
                    old_value = log.split(', ')[-2]
                    self.RM_Write(T, di, old_value, att_cache=False)
                    undo_updates.append((di, old_value))

        return undo_updates

    def RM_Restart(self):

        dict_results = {}
        print("Aborted: ", self.db.aborted_transactions)
        print("Active transactions: ", self.db.active_transactions)
        print("Consolidated transactions: ", self.db.consolidated_transactions)

        if len(self.db.aborted_transactions) > 0:
            for T in self.db.aborted_transactions:
                undo_aborted_updates = self._undo(T)
                dict_results['aborted'] = undo_aborted_updates
        
        if len(self.db.active_transactions) > 0:
            for T in self.db.active_transactions:
                undo_active_updates = self._undo(T)
                dict_results['active'] = undo_active_updates

        if len(self.db.consolidated_transactions) > 0:            
            for T in self.db.consolidated_transactions:
                redo_consolidated_updates = self._redo(T)
                dict_results['consolidated'] = redo_consolidated_updates

        return dict_results


