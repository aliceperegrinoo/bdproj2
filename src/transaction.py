class Transaction:
    def __init__(self, db, id, data_item, steps=[]):
        self.db = db
        self.id = id
        self.data_item = data_item
        self.steps = steps