class Transaction:
    def __init__(self, db, id):
        self.db = db
        self.id = id

    def start_transaction(self, T):
        return f'start, T{T.id}'