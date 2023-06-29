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
        logs = []
        if T not in self.db.active_transactions:
            self.db.add_active_transactions_list(T)

        if T.steps[-1] == 'read_item':
            if len(self.db.cache_log) > 0: 
                print('entrou no read com cache')
                read_log = [log for log in self.db.cache_log if \
                            log.split(', ')[1] == f'T{T.id}' \
                                and log.split(', ')[0] == 'read_item' \
                                    and log.split(', ')[2] == data_item]
            else:
                print('entrou no read com disk')
                read_log = [log for log in self.db.disk_log if \
                            log.split(', ')[1] == f'T{T.id}' \
                                and log.split(', ')[0] == 'read_item' \
                                    and log.split(', ')[2] == data_item]
            
            read_log = set(read_log)
            read_log = list(read_log)
            old_value = read_log[0].split(', ')[-1]

        elif T.steps[-1] == 'write_item':
            print("Cache log depois da falha: ", self.db.cache_log)
            if len(self.db.cache_log) > 0: 
                print('entrou no write com cache')
                write_logs = [log for log in self.db.cache_log if \
                            log.split(', ')[1] == f'T{T.id}' \
                                and log.split(', ')[0] == 'write_item' \
                                    and log.split(', ')[2] == data_item]
            else:
                print('entrou no write com disk')
                write_logs = [log for log in self.db.disk_log if \
                            log.split(', ')[1] == f'T{T.id}' \
                                and log.split(', ')[0] == 'write_item' \
                                    and log.split(', ')[2] == data_item]
            print("Write logs no write fazendo undo: ", write_logs)
            print("Disk log no write fazendo undo: ", self.db.disk_log)
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
        logs_to_sync = self.db.sync_cache_and_disk(T)
        logs_to_sync = self.db.check_for_duplicates_disk_log(logs_to_sync)
        if len(logs_to_sync) > 0:
            logs.extend(logs_to_sync)
        log = f'write_item, T{T.id}, {data_item}, {old_value}, {new_value}'
        logs.append(log)
        if att_cache == True:
            self.db.att_cache_log(log)
        for l in logs:
            self.db.att_disk_log(l)
        return logs
        
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

        if ('start' in T.steps) & ('read_item' not in T.steps):
            if len(T.data_item) > 0:
                for di in T.data_item:
                    logs.append(self.RM_Read(T, di))

        if 'write_item' in T.steps:
            filtered_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{T.id}']
            ImAn = filtered_log[0].split(', ')[-2]
            data_item = filtered_log[0].split(', ')[-3]
            logs.extend(self.RM_Write(T, data_item, ImAn))

        self.db.add_aborted_transactions_list(T)
        self.db.remove_active_transactions_list(T)
        for l in logs:
            self.db.att_cache_log(l)
        return logs
    
    def _undo(self, T):
        undo_updates = []
        for di in T.data_item:
            print("Disk log no undo: ", self.db.disk_log)
            filtered_logs = [log for log in self.db.disk_log if f'T{T.id}' == log.split(', ')[1] \
                              and not log.startswith('start') \
                                 and not log.startswith('aborted') \
                                     and not log.startswith('end') \
                                          and not log.startswith('commit') \
                                            and log.split(', ')[2] == di]
            print("Filtered logs no undo: ", filtered_logs)
            for log in reversed(filtered_logs):
                step = log.split(', ')[0]
                if step == 'write_item':
                    old_value = log.split(', ')[-2]
                    self.RM_Write(T, di, old_value, att_cache=False)
                    undo_updates.append((di, old_value))

        return undo_updates

    def RM_Restart(self):
        dict_results = {}
        not_consolidated = set(self.db.active_transactions).union(set(self.db.aborted_transactions)) - set(self.db.consolidated_transactions)
        not_consolidated = list(not_consolidated)

        if len(not_consolidated) > 0:
            for T in not_consolidated:
                undo_not_consolidated_transactions = self._undo(T)
                dict_results['not_consolidated'] = undo_not_consolidated_transactions

        return dict_results
    

 
    