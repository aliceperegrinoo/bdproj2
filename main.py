import sys
import time
from PyQt5.QtWidgets import QLineEdit, QComboBox, QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QTextEdit, QTableWidget, QTableWidgetItem, QLabel, QGridLayout, QRadioButton
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
        self.transaction = ""      
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
        self.radio_read.setEnabled(False)
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

        # Dropdown para exibir item de dados
        self.dict_dropdown = {}

        self.combobox_dataitem = QComboBox(self)
        for i, k in enumerate(self.db.data.keys()):
            self.combobox_dataitem.addItem(k)

            self.dict_dropdown[k] = i

        # Dropdown para exibir transações disponíveis
        self.combobox_transactions = QComboBox(self)

        # Text box para adicionar novo valor ao item de dados
        self.textbox = QLineEdit(self)
        self.textbox.setFixedWidth(150)

        # Layout
        layout = QGridLayout()
        layout.addWidget(self.recovery_label, 0, 0)
        layout.addWidget(self.radio_undo_no_redo, 1, 0)
        layout.addWidget(self.radio_undo_redo, 2, 0)
        layout.addWidget(self.data_item_label, 3, 0)
        layout.addWidget(self.combobox_dataitem, 4, 0)
        layout.addWidget(self.btn_start_transaction, 5, 0)
        layout.addWidget(self.radio_read, 6, 0)
        layout.addWidget(self.radio_write, 7, 0)
        layout.addWidget(self.textbox, 8, 0)
        layout.addWidget(self.btn_fail, 9, 0)
        layout.addWidget(self.btn_checkpoint, 10, 0)
        layout.addWidget(self.combobox_transactions, 11, 0)        
        layout.addWidget(self.btn_abort, 12, 0)
        layout.addWidget(self.btn_commit, 13, 0)
        layout.addWidget(self.btn_finish_transaction, 14, 0)
        layout.addWidget(self.btn_recover, 15, 0)
        layout.addWidget(self.log_memory_label, 0, 1)
        layout.addWidget(self.log_memory_display, 1, 1, 4, 1)
        layout.addWidget(self.log_disk_label, 6, 1)
        layout.addWidget(self.log_disk_display, 7, 1, 4, 1)
        layout.addWidget(self.db_table_label, 14, 1)
        layout.addWidget(self.db_table, 15, 1, 2, 1)

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
        
    def undoredo_recovery(self):
        self.recovery_mode = UndoRedoRecovery(self.db)

    def undonoredo_recovery(self):
        self.recovery_mode = UndoNoRedoRecovery(self.db)

    def start_recovery(self):
        self.log_disk_display.append('starting recovery...')
        time.sleep(1)
        logs = self.recovery_mode.RM_Restart()
        for log in logs:
            if log.split(', ')[0] == 'write_item':
                data_item = log.split(', ')[2]
                new_value = log.split(', ')[-1]
                self.update_db_table(self.dict_dropdown[data_item], new_value)
            self.log_disk_display.append(log)
        
    def perform_read(self):
        data_item = str(self.combobox_dataitem.currentText())
        log = self.recovery_mode.RM_Read(self.transaction, data_item)

        if self.recovery_mode.name == 'UndoNoRedoRecovery':
            self.log_memory_display.append(log)
            self.log_disk_display.append(log)
        if self.recovery_mode.name == 'UndoRedoRecovery':
            self.log_memory_display.append(log)

        self.transaction.steps.append('read_item')

        time.sleep(0.1)  
        self.radio_write.setEnabled(True)

    def perform_write(self):
        data_item = str(self.combobox_dataitem.currentText())
        new_value = str(self.textbox.text())
        log = self.recovery_mode.RM_Write(self.transaction, data_item, new_value)

        if self.recovery_mode.name == 'UndoNoRedoRecovery':
            self.log_memory_display.append(log)
            self.log_disk_display.append(log)
            self.update_db_table(self.dict_dropdown[data_item], new_value)
        if self.recovery_mode.name == 'UndoRedoRecovery':
            self.log_memory_display.append(log)

        self.transaction.steps.append('write_item')

        time.sleep(0.1)
        self.radio_read.setEnabled(False)
        self.btn_commit.setEnabled(True)

    def start_transaction(self):
        self.transaction_id += 1
        data_item = str(self.combobox_dataitem.currentText())
        self.transaction = Transaction(self.db, self.transaction_id, data_item)
        self.transactions.append(self.transaction)
        self.db.add_active_transactions_list(self.transaction)
        self.update_dropdown_transactions()
        log = f'start, T{self.transaction_id}'
        self.transaction.steps.append('start')
        self.log_memory_display.append(log)
        self.log_disk_display.append(log)
        self.radio_read.setEnabled(True)

        if self.recovery_mode.name == 'UndoNoRedoRecovery':
            self.btn_commit.setEnabled(False)

    def update_dropdown_transactions(self):
        self.combobox_transactions.clear()
        for transaction in self.transactions:
            self.combobox_transactions.addItem(f'T{transaction.id}')
        
    def perform_fail(self):
        self.log_memory_display.clear()

    def perform_checkpoint(self):
        active_transactions = [f'T{t.id}' for t in self.db.active_transactions]
        self.log_disk_display.append(f'checkpoint, {active_transactions}')
        add_to_disk = set(self.db.cache_log) - set(self.db.disk_log)
        if len(list(add_to_disk)) > 0:
            self.db.disk_log.extend(list(add_to_disk))
            # self.log_memory_display.clear()

    def perform_abort(self):
        current_transaction = str(self.combobox_transactions.currentText())
        current_object_transaction = [T for T in self.transactions if f'T{T.id}' == current_transaction] 
        logs = self.recovery_mode.RM_Abort(current_object_transaction[0])
        for log in logs:
            self.log_memory_display.append(log)
            time.sleep(1)
            self.log_disk_display.append(log)

        if 'write_item' in self.transaction.steps:
            filtered_logs = [log for log in logs if log.split(', ')[0] == 'write_item' and log.split(', ')[1] == f'T{current_object_transaction[0].id}']
            data_item = self.transaction.data_item
            new_value = filtered_logs[0].split(', ')[-1]
            self.update_db_table(self.dict_dropdown[data_item], new_value)

    def perform_commit(self):
        log = self.recovery_mode.RM_Commit(self.transaction)
        self.log_memory_display.append(log)
        self.log_disk_display.append(log)

    def finish_transaction(self):
        self.log_memory.append(f"FINISH TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(f"FINISH TRANSACTION {self.transaction_id}")
        self.transaction_id = 0

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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RecoveryInterface()
    window.show()
    sys.exit(app.exec_())
