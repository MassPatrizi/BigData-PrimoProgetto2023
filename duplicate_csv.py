import pandas as pd

input_file = 'ReviewsCleaned.csv'  # Specifica il percorso del tuo file CSV di input
output_file = 'ReviewsCleaned.csv'  # Specifica il percorso del file CSV di output

# Leggi il file CSV di input con delimitatore ';'
df = pd.read_csv(input_file, delimiter=';')

# Duplica, quintuplica e decuplica il dataframe
duplicated_df = pd.concat([df] * 2, ignore_index=True)  # Duplica le righe
quintupled_df = pd.concat([df] * 5, ignore_index=True)  # Quintuplica le righe
decupled_df = pd.concat([df] * 10, ignore_index=True)  # Decuplica le righe

# Salva i dataframe nei file CSV di output con delimitatore ';'
duplicated_df.to_csv('duplicated_' + output_file, index=False, sep=';')
quintupled_df.to_csv('quintupled_' + output_file, index=False, sep=';')
decupled_df.to_csv('decupled_' + output_file, index=False, sep=';')

print("Operazioni completate.")
