import sys
import time
from PyQt5.QtWidgets import QLineEdit, QComboBox, QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QTextEdit, QTableWidget, QTableWidgetItem, QLabel, QGridLayout, QRadioButton, QMessageBox
from PyQt5.QtCore import Qt

from src.recovery.undoredo import UndoRedoRecovery
from src.recovery.undonoredo import UndoNoRedoRecovery
from src.database import Database
from src.transaction import Transaction

class RecoveryInterface(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SGBD - Recuperação Imediata")
        self.setGeometry(100, 100, 800, 800)

        # Variáveis do SGBD
        self.transaction_id = 0
        self.db = Database(data={'x': "2", 'y': "5", "z": "10"})
        self.log_memory = []
        self.log_disk = []
        self.recovery_mode = ""  
        self.transactions = []
        self.initial_state = self.db.data.copy()
        self.updated_state = self.db.data.copy()

        # Botões de operações
        self.btn_start_transaction = QPushButton("Iniciar Transação", self)
        self.btn_fail = QPushButton("Falha", self)
        self.btn_checkpoint = QPushButton("Checkpoint", self)
        self.btn_abort = QPushButton("Abortar", self)
        self.btn_commit = QPushButton("Commit", self)
        self.btn_finish_transaction = QPushButton("Terminar Transação", self)
        self.btn_recover = QPushButton("Recuperar", self)
        self.btn_restart = QPushButton("Reiniciar", self)

        # Radio buttons para selecionar o read/write
        self.radio_read = QPushButton("Read", self)
        self.radio_write = QPushButton("Write", self)

        # Radio buttons para selecionar o algoritmo de recuperação
        self.radio_undo_no_redo = QRadioButton("UNDO/NO-REDO", self)
        self.radio_undo_redo = QRadioButton("UNDO/REDO", self)

        # Titulo algoritmo recuperação
        self.recovery_label = QLabel("Algoritmo de recuperação")
        self.recovery_label.setContentsMargins(0, 0, 0, 0)
        self.recovery_label.setFixedWidth(150)
        self.recovery_label.setWordWrap(True)

        self.data_item_label = QLabel("Selecione o item de dado:")
        self.data_item_label.setContentsMargins(0, 0, 0, 0)
        self.data_item_label.setFixedWidth(150)
        self.data_item_label.setWordWrap(True)

        # Área de texto para exibir o log de memória
        self.log_memory_label = QLabel("Log da Cache", self)
        # self.log_memory_label.move(420,0)
        self.log_memory_label.setAlignment(Qt.AlignCenter)
        self.log_memory_display = QTextEdit(self)
        self.log_memory_display.setReadOnly(True)

        # Área de texto para exibir o log de disco
        self.log_disk_label = QLabel("Log do Disco", self)
        self.log_disk_label.setAlignment(Qt.AlignCenter)
        self.log_disk_display = QTextEdit(self)
        self.log_disk_display.setReadOnly(True)

        # Tabela para exibir o banco de dados
        self.db_table_label = QLabel("Banco de Dados", self)
        self.db_table_label.setAlignment(Qt.AlignCenter)
        self.db_table = QTableWidget(self)
        self.db_table.setColumnCount(2)
        self.db_table.setHorizontalHeaderLabels(["Item", "Valor"])

        self.dict_dropdown = {}

        # Instanciando dropdowns
        self.combobox_dataitem = QComboBox(self)
        self.combobox_abort = QComboBox(self)
        self.combobox_commit = QComboBox(self)
        self.combobox_terminate = QComboBox(self)
        self.combobox_read = QComboBox(self)

        # Populando dropdowns
        for i, k in enumerate(self.db.data.keys()):
            self.combobox_dataitem.addItem(k)

            self.dict_dropdown[k] = i

        # Text box para adicionar novo valor ao item de dados
        self.textbox = QLineEdit(self)
        self.textbox.setFixedWidth(150)

        # Instanciando warnings
        self.commit_warning = QMessageBox()
        self.commit_warning.setWindowTitle("Processo não permitido")
        self.commit_warning.setText("Não é permitido commitar uma transação que não foi terminada. Por favor, finalize a transação antes.")
        self.commit_warning.setIcon(QMessageBox.Warning)

        self.readwrite_warning = QMessageBox()
        self.readwrite_warning.setWindowTitle("Processo não permitido")
        self.readwrite_warning.setText("Não é permitido realizar operações numa transação terminada!")
        self.readwrite_warning.setIcon(QMessageBox.Warning)

        self.terminate_warning = QMessageBox()
        self.terminate_warning.setWindowTitle("Processo não permitido")
        self.terminate_warning.setText("Não é permitido finalizar uma transação já finalizada!")
        self.terminate_warning.setIcon(QMessageBox.Warning)

        self.write_warning = QMessageBox()
        self.write_warning.setWindowTitle("Processo não permitido")
        self.write_warning.setText("Não é permitido realizar uma operação de escrita num item de dado sem antes realizar uma operação de leitura desse item. Por favor, leia o item primeiro.")
        self.write_warning.setIcon(QMessageBox.Warning)

        self.recovery_warning = QMessageBox()
        self.recovery_warning.setWindowTitle("Processo não permitido")
        self.recovery_warning.setText("Por favor, selecione o algortimo de recuperação primeiro.")
        self.recovery_warning.setIcon(QMessageBox.Warning)

        self.no_aborted_commit_warning = QMessageBox()
        self.no_aborted_commit_warning.setWindowTitle("Processo não permitido")
        self.no_aborted_commit_warning.setText("Não é permitido abortar uma transação commitada.")
        self.no_aborted_commit_warning.setIcon(QMessageBox.Warning)

        # Layout
        layout = QGridLayout()
        layout.addWidget(self.recovery_label, 0, 0)
        layout.addWidget(self.radio_undo_no_redo, 1, 0)
        layout.addWidget(self.radio_undo_redo, 2, 0)
        layout.addWidget(self.data_item_label, 3, 0)
        layout.addWidget(self.combobox_dataitem, 4, 0)
        layout.addWidget(self.btn_start_transaction, 5, 0)
        layout.addWidget(self.combobox_read, 6, 0)
        layout.addWidget(self.radio_read, 7, 0)
        layout.addWidget(self.radio_write, 8, 0)
        layout.addWidget(self.textbox, 9, 0)
        layout.addWidget(self.btn_fail, 10, 0)
        layout.addWidget(self.btn_checkpoint, 11, 0)
        layout.addWidget(self.combobox_abort, 12, 0) 
        layout.addWidget(self.btn_abort, 13, 0)
        layout.addWidget(self.combobox_commit, 14, 0)
        layout.addWidget(self.btn_commit, 15, 0)
        layout.addWidget(self.combobox_terminate, 16, 0)
        layout.addWidget(self.btn_finish_transaction, 17, 0)
        layout.addWidget(self.btn_recover, 18, 0)
        layout.addWidget(self.btn_restart, 21, 0)
        layout.addWidget(self.log_memory_label, 0, 1)
        layout.addWidget(self.log_memory_display, 1, 1, 5, 1)        
        layout.addWidget(self.log_disk_label, 7, 1)
        layout.addWidget(self.log_disk_display, 8, 1, 5, 1)        
        layout.addWidget(self.db_table_label, 14, 1)
        layout.addWidget(self.db_table, 15, 1, 8, 1)

       # Widget principal
        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        # Preencher a tabela com os dados do banco de dados
        self.create_db_table(db=self.db.data)

        # Conectar sinais aos slots
        self.btn_start_transaction.clicked.connect(self.start_transaction)
        self.radio_read.clicked.connect(self.perform_read)
        self.radio_write.clicked.connect(self.perform_write)
        self.btn_fail.clicked.connect(self.perform_fail)
        self.btn_checkpoint.clicked.connect(self.perform_checkpoint)
        self.btn_abort.clicked.connect(self.perform_abort)
        self.btn_commit.clicked.connect(self.perform_commit)
        self.btn_finish_transaction.clicked.connect(self.finish_transaction)
        self.btn_recover.clicked.connect(self.start_recovery)
        self.radio_undo_redo.clicked.connect(self.undoredo_recovery)
        self.radio_undo_no_redo.clicked.connect(self.undonoredo_recovery)
        self.btn_restart.clicked.connect(self.restart_program)

    def start_transaction(self):
        if self.recovery_mode == '':
            self.recovery_warning.exec_()
        else:
            self.transaction_id += 1
            data_item = str(self.combobox_dataitem.currentText())
            T = Transaction(self.db, self.transaction_id, data_item, steps=[])
            self.update_db_table_on_checkpoint(db=self.updated_state)
            self.transactions.append(T)
            self.update_dropdown_abort()
            self.update_dropdown_commit()
            self.update_dropdown_terminate()
            self.update_dropdown_read()
            log = self.recovery_mode.start_transaction(T)

            self.log_memory_display.append(log)
        
    def perform_read(self):
        current_transaction = str(self.combobox_read.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]

        if 'end' in T.steps:
            self.readwrite_warning.exec_()

        else:
            data_item = str(self.combobox_dataitem.currentText())
            log = self.recovery_mode.RM_Read(T, data_item)

            self.log_memory_display.append(log)

        print("Log disk read: ", self.db.disk_log)

    def perform_write(self):
        current_transaction = str(self.combobox_read.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]

        data_item = T.data_item
        read_log = [log for log in self.db.cache_log if log.split(', ')[0] == 'read_item' and log.split(', ')[1] == f'T{T.id}' and log.split(', ')[2] == data_item]
        
        if len(read_log) > 0:
            if self.recovery_mode.name == 'UndoRedoRecovery':
                if 'end' in T.steps:
                    self.readwrite_warning.exec_()
                else: 
                    data_item = str(self.combobox_dataitem.currentText())
                    new_value = str(self.textbox.text())
                    log = self.recovery_mode.RM_Write(T, data_item, new_value)

                    self.log_memory_display.append(log)

            elif self.recovery_mode.name == 'UndoNoRedoRecovery':
                if 'end' in T.steps:
                    self.readwrite_warning.exec_()
                else: 
                    data_item = str(self.combobox_dataitem.currentText())
                    new_value = str(self.textbox.text())
                    logs = self.recovery_mode.RM_Write(T, data_item, new_value)
                    self.log_memory_display.append(logs[-1])
                    for log in logs:
                        self.log_disk_display.append(log)
                        self.update_db_table(self.dict_dropdown[data_item], new_value)
                        self.updated_state[data_item] = new_value

            print("Log disk write: ", self.db.disk_log)
        else:
            self.write_warning.exec_()

    def finish_transaction(self):
        current_transaction = str(self.combobox_terminate.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]
        if 'end' not in T.steps:
            log = self.recovery_mode.terminate_transaction(T)
            self.log_memory_display.append(log)
        else:
            self.terminate_warning.exec_()

        print("Log disk finish: ", self.db.disk_log)

    def perform_commit(self):
        current_transaction = str(self.combobox_commit.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]
        if 'end' in T.steps:
            logs = self.recovery_mode.RM_Commit(T, type='default')
            for l in logs:
                self.log_disk_display.append(l)
                if l.split(', ')[0] == 'write_item':
                    data_item = T.data_item
                    new_value = l.split(', ')[-1]
                    self.update_db_table(self.dict_dropdown[data_item], new_value)
        else:
            self.commit_warning.exec_()

        print("Log disk commit: ", self.db.disk_log)

    def perform_fail(self):
        self.log_memory_display.clear()
        self.db.cache_log = []
        self.return_to_checkpoint_state()

    def perform_checkpoint(self): 
        active_transactions = [f'T{t.id}' for t in self.db.active_transactions]

        if self.recovery_mode.name == 'UndoRedoRecovery':
            need_commit = []
            all_transactions = [f'T{tr.id}' for tr in self.transactions]
            for tr in all_transactions:
                if self._check_if_commit_is_needed(tr) == True:
                    need_commit.append(tr)

            add_to_disk = self.db.sync_cache_and_disk_on_checkpoint()
            print("Add to disk: ", add_to_disk)

            if len(add_to_disk) > 0:
                for log in add_to_disk:
                    str_tr = log.split(', ')[1]
                    event = log.split(', ')[0]

                    # atualiza a base de dados se tiver algum write_item
                    if event == 'write_item':
                        T = [tr for tr in self.transactions if f'T{tr.id}' == str_tr]
                        T = T[0]
                        data_item = T.data_item
                        new_value = log.split(', ')[-1]
                        self.update_db_table(self.dict_dropdown[data_item], new_value)
                        self.updated_state[data_item] = new_value

                    # adiciona um commit se a transação tiver sido finalizada
                    if str_tr in need_commit and event == 'end':
                        T = [tr for tr in self.transactions if f'T{tr.id}' == str_tr]
                        T = T[0]
                        self.db.att_disk_log(log)
                        self.log_disk_display.append(log)
                        commit_log = self.recovery_mode.RM_Commit(T, type='simplified')
                        self.db.att_disk_log(commit_log)
                        self.log_disk_display.append(commit_log)
                    else:
                        self.db.att_disk_log(log)
                        self.log_disk_display.append(log)

            self.db.att_disk_log(f'checkpoint, {active_transactions}')
            self.log_disk_display.append(f'checkpoint, {active_transactions}')

        elif self.recovery_mode.name == 'UndoNoRedoRecovery':
                    
            self.db.att_disk_log(f'checkpoint, {active_transactions}')
            self.log_disk_display.append(f'checkpoint, {active_transactions}')

        print("Log disk checkpoint: ", self.db.disk_log)

    def _check_if_commit_is_needed(self, str_tr):
        T = [tr for tr in self.transactions if f'T{tr.id}' == str_tr]
        T = T[0]
        if 'end' in T.steps and 'commit' not in T.steps:
            return True
        else:
            return False

    def perform_abort(self):
        current_transaction = str(self.combobox_abort.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction] 
        T = current_object_transaction[0]
        if 'commit' not in T.steps:
            self.no_aborted_commit_warning.exec_()
        else:
            logs = self.recovery_mode.RM_Abort(T)
            for log in logs:
                self.log_memory_display.append(log)
                self.log_disk_display.append(log)

            if 'write_item' in T.steps:
                filtered_logs = [log for log in logs if log.split(', ')[0] == 'write_item' and \
                                log.split(', ')[1] == f'T{T.id}']
                data_item = T.data_item
                new_value = filtered_logs[0].split(', ')[-1]
                self.update_db_table(self.dict_dropdown[data_item], new_value)

        print(self.db.disk_log)

    def restart_program(self):
        self.db.cache_log = []
        self.db.disk_log = []
        self.db.aborted_transactions = []
        self.db.active_transactions = []
        self.db.consolidated_transactions = []
        self.transactions = []
        self.log_disk_display.clear()
        self.log_memory_display.clear()
        self.combobox_abort.clear()
        self.combobox_commit.clear()
        self.combobox_terminate.clear()
        self.combobox_read.clear()
        self.db = Database(data={'x': "2", 'y': "5", "z": "10"})
        self.update_db_table_on_checkpoint(db=self.db.data)
        self.transaction_id = 0
        self.recovery_mode = ''
        self.updated_state = self.db.data.copy()

    def undoredo_recovery(self):
        self.recovery_mode = UndoRedoRecovery(self.db)

    def undonoredo_recovery(self):
        self.recovery_mode = UndoNoRedoRecovery(self.db)

    def start_recovery(self):
        results = self.recovery_mode.RM_Restart()
        if 'aborted' in results.keys():
            for data_item, value in results['aborted']:
                print(data_item, value)
                self.update_db_table(self.dict_dropdown[data_item], value)
                self.updated_state[data_item] = value

        if 'active' in results.keys():
            for data_item, value in results['active']:
                print(data_item, value)
                self.update_db_table(self.dict_dropdown[data_item], value)
                self.updated_state[data_item] = value

        if 'consolidated' in results.keys():
            print(data_item, value)
            for data_item, value in results['consolidated']:
                self.update_db_table(self.dict_dropdown[data_item], value)
                self.updated_state[data_item] = value                

    def update_dropdown_read(self):
        self.combobox_read.clear()
        for transaction in self.transactions:
            self.combobox_read.addItem(f'T{transaction.id}')

    def update_dropdown_abort(self):
        self.combobox_abort.clear()
        for transaction in self.transactions:
            self.combobox_abort.addItem(f'T{transaction.id}')

    def update_dropdown_commit(self):
        self.combobox_commit.clear()
        for transaction in self.transactions:
            self.combobox_commit.addItem(f'T{transaction.id}')

    def update_dropdown_terminate(self):
        self.combobox_terminate.clear()
        for transaction in self.transactions:
            self.combobox_terminate.addItem(f'T{transaction.id}')

    def create_db_table(self, db):
        self.db_table.setRowCount(len(db))
        row = 0
        for item, value in db.items():
            item_widget = QTableWidgetItem(item)
            item_widget.setFlags(Qt.ItemIsEnabled)
            value_widget = QTableWidgetItem(value)
            self.db_table.setItem(row, 0, item_widget)
            self.db_table.setItem(row, 1, value_widget)
            row += 1

    def update_db_table(self, row, value, column=1):
        item = QTableWidgetItem(str(value))
        self.db_table.setItem(row, column, item)

    def update_db_table_on_checkpoint(self, db):
        for i, val in enumerate(db.values()):
            item = QTableWidgetItem(val)
            self.db_table.setItem(i, 1, item)

    def return_to_checkpoint_state(self):
        log_checkpoint = [log for log in self.db.disk_log if log.startswith("checkpoint")]
        if len(log_checkpoint) == 0:
            self.update_db_table_on_checkpoint(db=self.db.data)
        if len(log_checkpoint) > 0:
            self.update_db_table_on_checkpoint(db=self.updated_state)

        return

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RecoveryInterface()
    window.show()
    sys.exit(app.exec_())

