import sys
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
        self.log_memory = self.db.cache_log
        self.log_disk = self.db.disk_log

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
        combobox_read = QComboBox(self)
        combobox_write = QComboBox(self)
        for k in self.db.data.keys():
            combobox_read.addItem(k)
            combobox_write.addItem(k)

        # Text box para adicionar novo valor ao item de dados
        self.textbox = QLineEdit("Novo valor", self)
        self.textbox.setFixedWidth(120)

        # Layout
        layout = QGridLayout()
        # layout.addWidget(self.transaction_labels, 0, 0)
        layout.addWidget(self.radio_read, 1, 0)
        layout.addWidget(combobox_read, 2, 0)
        layout.addWidget(self.radio_write, 3, 0)
        layout.addWidget(combobox_write, 4, 0)
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
        self.update_db_table()

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

    def perform_read(self):
        current_row = self.db_table.currentRow()
        if current_row >= 0:
            item = self.db_table.item(current_row, 0).text()
            value = self.db[item]
            self.log_memory.append(f"READ {item}: {value}")
            self.log_memory_display.append(f"READ {item}: {value}")

    def perform_write(self):
        current_row = self.db_table.currentRow()
        if current_row >= 0:
            item = self.db_table.item(current_row, 0).text()
            value = self.db_table.item(current_row, 1).text()
            self.db[item] = value
            self.log_memory.append(f"WRITE {item}: {value}")
            self.log_memory_display.append(f"WRITE {item}: {value}")

    def create_transaction(self):
        self.transaction_id += 1
        self.transaction = Transaction(self.db, self.transaction_id, ['write_item'], 'x')
        self.log_memory.append(f"CREATE TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(self.transaction.start())
        self.btn_start_transaction.clicked.connect(self.transaction.start())

    def start_transaction(self):
        self.log_memory.append(f"START TRANSACTION {self.transaction_id}")
        self.log_memory_display.append(f"START TRANSACTION {self.transaction_id}")

    def perform_fail(self):
        self.log_memory.append("FAIL")
        self.log_memory_display.append("FAIL")

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

    def update_db_table(self):
        self.db_table.setRowCount(len(self.db.data))
        row = 0
        for item, value in self.db.data.items():
            item_widget = QTableWidgetItem(item)
            item_widget.setFlags(Qt.ItemIsEnabled)
            value_widget = QTableWidgetItem(value)
            self.db_table.setItem(row, 0, item_widget)
            self.db_table.setItem(row, 1, value_widget)
            row += 1

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RecoveryInterface()
    window.show()
    sys.exit(app.exec_())
