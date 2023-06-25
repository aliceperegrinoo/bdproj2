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

        # Botões de operações
        self.btn_start_transaction = QPushButton("Iniciar Transação", self)
        self.btn_fail = QPushButton("Falha", self)
        self.btn_checkpoint = QPushButton("Checkpoint", self)
        self.btn_abort = QPushButton("Abortar", self)
        self.btn_commit = QPushButton("Commit", self)
        self.btn_finish_transaction = QPushButton("Terminar Transação", self)
        self.btn_recover = QPushButton("Recuperar", self)

        # Radio buttons para selecionar o read/write
        self.radio_read = QRadioButton("Read", self)
        self.radio_write = QRadioButton("Write", self)
        self.radio_write.setEnabled(False)

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
        layout.addWidget(self.log_memory_label, 0, 1)
        layout.addWidget(self.log_memory_display, 1, 1, 4, 1)
        layout.addWidget(self.log_disk_label, 8, 1)
        layout.addWidget(self.log_disk_display, 9, 1, 4, 1)
        layout.addWidget(self.db_table_label, 17, 1)
        layout.addWidget(self.db_table, 18, 1, 2, 1)

        # Widget principal
        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        # Preencher a tabela com os dados do banco de dados
        self.create_db_table()

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

    def start_transaction(self):
        self.radio_read.setEnabled(False)
        self.transaction_id += 1
        data_item = str(self.combobox_dataitem.currentText())
        T = Transaction(self.db, self.transaction_id, data_item, steps=[])
        self.transactions.append(T)
        self.update_dropdown_abort()
        self.update_dropdown_commit()
        self.update_dropdown_terminate()
        self.update_dropdown_read()
        log = self.recovery_mode.start_transaction(T)
        self.radio_read.setEnabled(True)

        self.log_memory_display.append(log)

        if self.recovery_mode.name == 'UndoNoRedoRecovery':
            self.btn_commit.setEnabled(False)
        
    def perform_read(self):
        current_transaction = str(self.combobox_read.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]

        if 'end' in T.steps:
            self.readwrite_warning.exec_()

        else:
            data_item = str(self.combobox_dataitem.currentText())
            log = self.recovery_mode.RM_Read(T, data_item)
            self.radio_write.setEnabled(True)

            self.log_memory_display.append(log)

    def perform_write(self):
        current_transaction = str(self.combobox_read.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]

        if 'end' in T.steps:
            self.readwrite_warning.exec_()
        else: 
            data_item = str(self.combobox_dataitem.currentText())
            new_value = str(self.textbox.text())
            log = self.recovery_mode.RM_Write(T, data_item, new_value)

            self.log_memory_display.append(log)

            self.radio_read.setEnabled(False)

    def finish_transaction(self):
        current_transaction = str(self.combobox_terminate.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]
        if 'end' not in T.steps:
            log = self.recovery_mode.terminate_transaction(T)
            self.log_memory_display.append(log)
        else:
            self.terminate_warning.exec_()

    def perform_commit(self):
        current_transaction = str(self.combobox_commit.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction]
        T = current_object_transaction[0]
        if 'end' in T.steps:
            logs = self.recovery_mode.RM_Commit(T)
            for l in logs:
                print(l)
                self.log_disk_display.append(l)
                if l.split(', ')[0] == 'write_item':
                    data_item = T.data_item
                    new_value = l.split(', ')[-1]
                    self.update_db_table(self.dict_dropdown[data_item], new_value)
        else:
            self.commit_warning.exec_()

    def perform_fail(self):
        self.log_memory_display.clear()

    def perform_checkpoint(self):
        need_commit = []
        all_transactions = [f'T{tr.id}' for tr in self.transactions]
        for tr in all_transactions:
            if self._check_if_commit_is_needed(tr) == True:
                need_commit.append(tr)

        active_transactions = [f'T{t.id}' for t in self.db.active_transactions]

        add_to_disk = self.db.sync_cache_and_disk_on_checkpoint()

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

        self.log_disk_display.append(f'checkpoint, {active_transactions}')

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
        logs = self.recovery_mode.RM_Abort(current_object_transaction[0])
        for log in logs:
            self.log_memory_display.append(log)
            time.sleep(1)
            self.log_disk_display.append(log)

        if 'write_item' in self.transaction.steps:
            filtered_logs = [log for log in logs if log.split(', ')[0] == 'write_item' and \
                              log.split(', ')[1] == f'T{current_object_transaction.id}']
            data_item = self.transaction.data_item
            new_value = filtered_logs[0].split(', ')[-1]
            self.update_db_table(self.dict_dropdown[data_item], new_value)

    def undoredo_recovery(self):
        self.recovery_mode = UndoRedoRecovery(self.db)

    def undonoredo_recovery(self):
        self.recovery_mode = UndoNoRedoRecovery(self.db)

    def start_recovery(self):
        logs = self.recovery_mode.RM_Restart()
        for log in logs:
            if log.split(', ')[0] == 'write_item':
                data_item = log.split(', ')[2]
                new_value = log.split(', ')[-1]
                self.update_db_table(self.dict_dropdown[data_item], new_value)
            self.log_disk_display.append(log)

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

    def create_db_table(self):
        self.db_table.setRowCount(len(self.db.data))
        row = 0
        for item, value in self.db.data.items():
            item_widget = QTableWidgetItem(item)
            item_widget.setFlags(Qt.ItemIsEnabled)
            value_widget = QTableWidgetItem(value)
            self.db_table.setItem(row, 0, item_widget)
            self.db_table.setItem(row, 1, value_widget)
            row += 1

    def update_db_table(self, row, value, column=1):
        item = QTableWidgetItem(str(value))
        self.db_table.setItem(row, column, item)

    def get_table_state_from_checkpoint(self):
        return

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RecoveryInterface()
    window.show()
    sys.exit(app.exec_())
