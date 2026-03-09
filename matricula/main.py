import pandas as pd

# archivo de entrada
archivo_csv = "matricula.csv"

# archivo de salida
archivo_excel = "noveno_grado.xlsx"

# leer CSV
df = pd.read_csv(archivo_csv)

# filtrar Noveno Grado
df_noveno = df[df["GRADO"].str.strip() == "Noveno Grado"]

# exportar a Excel
df_noveno.to_excel(archivo_excel, index=False)

print("Archivo generado:", archivo_excel)