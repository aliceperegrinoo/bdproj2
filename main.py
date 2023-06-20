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

        # # Título barra lateral
        # self.transaction_labels = QLabel("Criar transação", self)
        # self.transaction_labels.setAlignment(Qt.AlignCenter)

        # Área de texto para exibir o log de memória
        self.log_memory_label = QLabel("Log da Cache", self)
        self.log_memory_label.move(420,0)
        # self.log_memory_label.setAlignment(Qt.AlignCenter)
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

        self.combobox_read = QComboBox(self)
        self.combobox_write = QComboBox(self)
        for i, k in enumerate(self.db.data.keys()):
            self.combobox_read.addItem(k)
            self.combobox_write.addItem(k)

            self.dict_dropdown[k] = i

        # Text box para adicionar novo valor ao item de dados
        self.textbox = QLineEdit(self)
        self.textbox.setFixedWidth(120)

        # Layout
        layout = QGridLayout()
        # layout.addWidget(self.transaction_labels, 0, 0)
        layout.addWidget(self.radio_read, 1, 0)
        layout.addWidget(self.combobox_read, 2, 0)
        layout.addWidget(self.radio_write, 3, 0)
        layout.addWidget(self.combobox_write, 4, 0)
        layout.addWidget(self.textbox, 5, 0)
        layout.addWidget(self.btn_start_transaction, 6, 0)
        layout.addWidget(self.btn_fail, 7, 0)
        layout.addWidget(self.btn_checkpoint, 8, 0)
        layout.addWidget(self.btn_abort, 9, 0)
        layout.addWidget(self.btn_commit, 10, 0)
        layout.addWidget(self.btn_finish_transaction, 11, 0)
        layout.addWidget(self.radio_undo_no_redo, 12, 0)
        layout.addWidget(self.radio_undo_redo, 13, 0)
        layout.addWidget(self.btn_recover, 14, 0)
        # layout.addWidget(self.log_memory_label, 0, 1)
        layout.addWidget(self.log_memory_display, 2, 1, 4, 1)
        layout.addWidget(self.log_disk_label, 7, 1)
        layout.addWidget(self.log_disk_display, 8, 1, 4, 1)
        layout.addWidget(self.db_table_label, 13, 1)
        layout.addWidget(self.db_table, 14, 1, 2, 1)

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
        self.btn_recover.clicked.connect(self.recover)
        self.radio_undo_redo.clicked.connect(self.undoredo_recovery)
        self.radio_undo_no_redo.clicked.connect(self.undonoredo_recovery)
        
    def undoredo_recovery(self):
        UndoRedoRecovery.RM_Restart()

    def undonoredo_recovery(self):
        UndoNoRedoRecovery.RM_Restart()

    def perform_read(self):
        data_item = str(self.combobox_read.currentText())
        log = UndoRedoRecovery.RM_Read(self, self.transaction, data_item)
        self.log_memory.append(log)
        self.log_memory_display.append(log)

        self.radio_write.setEnabled(True)

    def perform_write(self):
        data_item = str(self.combobox_write.currentText())
        new_value = str(self.textbox.text())
        log = UndoRedoRecovery.RM_Write(self, self.transaction, data_item, new_value)
        self.log_memory.append(log)
        self.log_memory_display.append(log)
        time.sleep(0.1)

        self.radio_read.setEnabled(False)
        self.update_db_table(self.dict_dropdown[data_item], new_value)

    def start_transaction(self):
        self.transaction_id += 1
        self.transaction = Transaction(self.db, self.transaction_id)
        log = f'start, T{self.transaction_id}'
        self.log_memory.append(log)
        self.log_memory_display.append(log)
        self.radio_read.setEnabled(True)
        
    def perform_fail(self):
        self.log_memory = []
        self.log_memory_display = []

    def perform_checkpoint(self):
        self.log_disk.extend(self.log_memory)
        self.log_memory = []
        self.log_memory_display.clear()
        self.log_disk_display.clear()
        for log in self.log_disk:
            self.log_disk_display.append(log)

    def perform_abort(self):
        self.log_memory.append(f"ABORT TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(f"ABORT TRANSACTION {self.transaction_id}")
        self.transaction_id = 0

    def perform_commit(self):
        self.log_memory.append(f"COMMIT TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(f"COMMIT TRANSACTION {self.transaction_id}")
        self.transaction_id = 0

    def finish_transaction(self):
        self.log_memory.append(f"FINISH TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(f"FINISH TRANSACTION {self.transaction_id}")
        self.transaction_id = 0

    def recover(self):
        selected_algorithm = ""
        if self.radio_undo_no_redo.isChecked():
            selected_algorithm = "UNDO/NO-REDO"
        elif self.radio_undo_redo.isChecked():
            selected_algorithm = "UNDO/REDO"
        self.log_memory.append(f"RECOVER using {selected_algorithm}")
        self.log_memory_display.append(f"RECOVER using {selected_algorithm}")

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
