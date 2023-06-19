class Transaction:
    def __init__(self, db, id, steps, data_item):
        self.db = db
        self.id = id
        self.steps = steps
        self.data_item = data_item