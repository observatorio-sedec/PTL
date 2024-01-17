data = "202303"

# Divida a string usando "/" como separador
partes = data.split("/")

# Obtenha o ano sem o trimestre

ano_sem_trimestre = int(partes[0][:4])
trimestre = int(partes[0][4:6])
# Imprima o ano sem o trimestre
print("Ano sem trimestre:", ano_sem_trimestre)
print('Semestre:', trimestre)

