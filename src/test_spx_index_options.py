
import wrds
db = wrds.Connection()

tables = db.list_tables(library='optionm')
print(tables)

db.close()
