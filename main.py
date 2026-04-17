import os
import csv
import time
import glob
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from collections import defaultdict


#Utilitarios

def encontrar_arquivos_csv(diretorio="."):
    """Retorna lista de todos os arquivos .csv no diretório."""
    return glob.glob(os.path.join(diretorio, "*.csv"))


def ler_csv(caminho):
    """Lê um arquivo CSV e retorna lista de dicionários."""
    linhas = []
    try:
        with open(caminho, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                linhas.append(dict(row))
    except Exception as e:
        print(f"Erro ao ler {caminho}: {e}")
    return linhas

#Salva lista de dicionários em arquivo CSV
def salvar_csv(caminho, dados, campos):
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(dados)
    print(f"Arquivo salvo: {caminho}")

#Converte valor para float de forma segura.
def safe_float(valor):
    try:
        return float(str(valor).replace(',', '.').strip())
    except (ValueError, TypeError):
        return 0.0

#Meta1 = (Σjulgados_2026 / (Σcasos_novos_2026 + Σdessobrestados_2026 - Σsuspensos_2026)) * 100.
def calcular_meta1(rows):
    julgados = sum(safe_float(r.get('julgados_2026', 0)) for r in rows)
    casos_novos = sum(safe_float(r.get('casos_novos_2026', 0)) for r in rows)
    dessobrestados = sum(safe_float(r.get('dessobrestados_2026', 0)) for r in rows)
    suspensos = sum(safe_float(r.get('suspensos_2026', 0)) for r in rows)
    denominador = casos_novos + dessobrestados - suspensos
    return (julgados / denominador * 100) if denominador != 0 else 0.0

#Meta2A = (Σjulgm2_a / (Σdistm2_a - Σsuspm2_a)) * (1000/7)
def calcular_meta2a(rows):
    julgm2_a = sum(safe_float(r.get('julgm2_a', 0)) for r in rows)
    distm2_a = sum(safe_float(r.get('distm2_a', 0)) for r in rows)
    suspm2_a = sum(safe_float(r.get('suspm2_a', 0)) for r in rows)
    denominador = distm2_a - suspm2_a
    return (julgm2_a / denominador * (1000 / 7)) if denominador != 0 else 0.0

#Meta2Ant = (Σjulgm2_ant / (Σdistm2_ant - Σsuspm2_ant - Σdesom2_ant)) * 100
def calcular_meta2ant(rows):
    julgm2_ant = sum(safe_float(r.get('julgm2_ant', 0)) for r in rows)
    distm2_ant = sum(safe_float(r.get('distm2_ant', 0)) for r in rows)
    suspm2_ant = sum(safe_float(r.get('suspm2_ant', 0)) for r in rows)
    desom2_ant = sum(safe_float(r.get('desom2_ant', 0)) for r in rows)
    denominador = distm2_ant - suspm2_ant - desom2_ant
    return (julgm2_ant / denominador * 100) if denominador != 0 else 0.0

#Meta4A = (Σjulgm4_a / (Σdistm4_a - Σsuspm4_a)) * 100
def calcular_meta4a(rows):
    julgm4_a = sum(safe_float(r.get('julgm4_a', 0)) for r in rows)
    distm4_a = sum(safe_float(r.get('distm4_a', 0)) for r in rows)
    suspm4_a = sum(safe_float(r.get('suspm4_a', 0)) for r in rows)
    denominador = distm4_a - suspm4_a
    return (julgm4_a / denominador * 100) if denominador != 0 else 0.0


#Meta4B = (Σjulgm4_b / (Σdistm4_b - Σsuspm4_b)) * 100
def calcular_meta4b(rows):
    julgm4_b = sum(safe_float(r.get('julgm4_b', 0)) for r in rows)
    distm4_b = sum(safe_float(r.get('distm4_b', 0)) for r in rows)
    suspm4_b = sum(safe_float(r.get('suspm4_b', 0)) for r in rows)
    denominador = distm4_b - suspm4_b
    return (julgm4_b / denominador * 100) if denominador != 0 else 0.0

#Calcula todas as metas para um grupo de linhas.
def calcular_metas_grupo(rows):
    return {
        'Meta1': round(calcular_meta1(rows), 4),
        'Meta2A': round(calcular_meta2a(rows), 4),
        'Meta2Ant': round(calcular_meta2ant(rows), 4),
        'Meta4A': round(calcular_meta4a(rows), 4),
        'Meta4B': round(calcular_meta4b(rows), 4),
    }

# CONCATENAR ARQUIVOS CSV
    """
    Concatena todos os arquivos CSV do diretório em um único arquivo.
    Versão serial.
    """

def func1_serial(diretorio="."):
    inicio = time.perf_counter()
    arquivos = encontrar_arquivos_csv(diretorio)
    if not arquivos:
        print("Nenhum arquivo CSV encontrado.")
        return

    todos_dados = []
    campos = None

    for arq in arquivos:
        linhas = ler_csv(arq)
        if linhas and campos is None:
            campos = list(linhas[0].keys())
        todos_dados.extend(linhas)

    if todos_dados and campos:
        salvar_csv("concatenado_serial.csv", todos_dados, campos)

    fim = time.perf_counter()
    print(f"[Func1 Serial] Tempo: {fim - inicio:.4f}s | Linhas: {len(todos_dados)}")
    return fim - inicio


def _ler_arquivo_worker(caminho):
    return ler_csv(caminho)


    """
    Concatena todos os arquivos CSV do diretório em um único arquivo.
    Versão paralela (ThreadPoolExecutor).
    """
def func1_paralela(diretorio="."):
    inicio = time.perf_counter()
    arquivos = encontrar_arquivos_csv(diretorio)
    if not arquivos:
        print("Nenhum arquivo CSV encontrado.")
        return

    num_workers = min(len(arquivos), multiprocessing.cpu_count() * 2)
    resultados = []
    campos = None

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(_ler_arquivo_worker, arq): arq for arq in arquivos}
        for future in futures:
            linhas = future.result()
            if linhas and campos is None:
                campos = list(linhas[0].keys())
            resultados.extend(linhas)

    if resultados and campos:
        salvar_csv("concatenado_paralelo.csv", resultados, campos)

    fim = time.perf_counter()
    print(f"[Func1 Paralela] Tempo: {fim - inicio:.4f}s | Linhas: {len(resultados)}")
    return fim - inicio



#  RESUMO POR MUNICÍPIO

def func2_serial(dados):
    """
    Gera resumo por municipio_oj com as 5 metas.
    Versão serial.
    """
    inicio = time.perf_counter()

    grupos = defaultdict(list)
    for row in dados:
        mun = row.get('municipio_oj', '').strip()
        if mun:
            grupos[mun].append(row)

    resumo = []
    for municipio, rows in grupos.items():
        metas = calcular_metas_grupo(rows)
        resumo.append({'municipio_oj': municipio, **metas})

    campos = ['municipio_oj', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']
    salvar_csv("resumo_municipio_serial.csv", resumo, campos)

    fim = time.perf_counter()
    print(f"[Func2 Serial] Tempo: {fim - inicio:.4f}s | Municípios: {len(resumo)}")
    return fim - inicio

#Worker para cálculo paralelo por município.
def _calcular_municipio(args):
    municipio, rows = args
    metas = calcular_metas_grupo(rows)
    return {'municipio_oj': municipio, **metas}

    """
    Gera resumo por municipio_oj com as 5 metas.
    Versão paralela (ProcessPoolExecutor).
    """
def func2_paralela(dados):
    inicio = time.perf_counter()

    grupos = defaultdict(list)
    for row in dados:
        mun = row.get('municipio_oj', '').strip()
        if mun:
            grupos[mun].append(row)

    num_workers = min(len(grupos), multiprocessing.cpu_count())
    resumo = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        resultados = list(executor.map(_calcular_municipio, grupos.items()))
        resumo.extend(resultados)

    campos = ['municipio_oj', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']
    salvar_csv("resumo_municipio_paralelo.csv", resumo, campos)

    fim = time.perf_counter()
    print(f"[Func2 Paralela] Tempo: {fim - inicio:.4f}s | Municípios: {len(resumo)}")
    return fim - inicio



# TOP 10 TRIBUNAIS POR META1

def func3_serial(dados):
    inicio = time.perf_counter()

    grupos = defaultdict(list)
    for row in dados:
        trib = row.get('sigla_tribunal', '').strip()
        if trib:
            grupos[trib].append(row)

    resumo = []
    for tribunal, rows in grupos.items():
        metas = calcular_metas_grupo(rows)
        resumo.append({'sigla_tribunal': tribunal, **metas})

    # Ordenar por Meta1 decrescente e pegar top 10
    resumo.sort(key=lambda x: x['Meta1'], reverse=True)
    top10 = resumo[:10]

    campos = ['sigla_tribunal', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']
    salvar_csv("top10_tribunais_serial.csv", top10, campos)

    fim = time.perf_counter()
    print(f"[Func3 Serial] Tempo: {fim - inicio:.4f}s | Top 10 tribunais gerado.")
    return fim - inicio

#Worker para cálculo paralelo por tribunal.
def _calcular_tribunal(args):
    tribunal, rows = args
    metas = calcular_metas_grupo(rows)
    return {'sigla_tribunal': tribunal, **metas}

    """
    Gera resumo dos 10 tribunais com maiores valores em Meta1.
    Versão paralela (ProcessPoolExecutor).
    """
def func3_paralela(dados):
    inicio = time.perf_counter()

    grupos = defaultdict(list)
    for row in dados:
        trib = row.get('sigla_tribunal', '').strip()
        if trib:
            grupos[trib].append(row)

    num_workers = min(len(grupos), multiprocessing.cpu_count())
    resumo = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        resultados = list(executor.map(_calcular_tribunal, grupos.items()))
        resumo.extend(resultados)

    resumo.sort(key=lambda x: x['Meta1'], reverse=True)
    top10 = resumo[:10]

    campos = ['sigla_tribunal', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']
    salvar_csv("top10_tribunais_paralelo.csv", top10, campos)

    fim = time.perf_counter()
    print(f"[Func3 Paralela] Tempo: {fim - inicio:.4f}s | Top 10 tribunais gerado.")
    return fim - inicio



# FUNCIONALIDADE 4 - FILTRAR POR MUNICÍPIO
    """
    Filtra todas as ocorrências de um município e salva em CSV.
    Versão serial.
    """

def func4_serial(dados, municipio):
    inicio = time.perf_counter()
    municipio_upper = municipio.strip().upper()

    filtrado = [
        row for row in dados
        if row.get('municipio_oj', '').strip().upper() == municipio_upper
    ]

    nome_arquivo = f"{municipio.strip().upper()}.csv"
    if filtrado:
        campos = list(filtrado[0].keys())
        salvar_csv(nome_arquivo, filtrado, campos)
    else:
        print(f"Nenhuma ocorrência encontrada para município: {municipio}")

    fim = time.perf_counter()
    print(f"[Func4 Serial] Tempo: {fim - inicio:.4f}s | Linhas filtradas: {len(filtrado)}")
    return fim - inicio


#Worker para filtro paralelo de chunk.
def _filtrar_chunk(args):
    chunk, municipio_upper = args
    return [row for row in chunk if row.get('municipio_oj', '').strip().upper() == municipio_upper]

    """
    Filtra todas as ocorrências de um município e salva em CSV.
    Versão paralela (ThreadPoolExecutor com divisão em chunks).
    """
def func4_paralela(dados, municipio):
    inicio = time.perf_counter()
    municipio_upper = municipio.strip().upper()

    # Dividir dados em chunks para processamento paralelo
    num_workers = multiprocessing.cpu_count()
    tamanho_chunk = max(1, len(dados) // num_workers)
    chunks = [dados[i:i + tamanho_chunk] for i in range(0, len(dados), tamanho_chunk)]

    filtrado = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = executor.map(_filtrar_chunk, [(chunk, municipio_upper) for chunk in chunks])
        for resultado in futures:
            filtrado.extend(resultado)

    nome_arquivo = f"{municipio.strip().upper()}_paralelo.csv"
    if filtrado:
        campos = list(filtrado[0].keys())
        salvar_csv(nome_arquivo, filtrado, campos)
    else:
        print(f"Nenhuma ocorrência encontrada para município: {municipio}")

    fim = time.perf_counter()
    print(f"[Func4 Paralela] Tempo: {fim - inicio:.4f}s | Linhas filtradas: {len(filtrado)}")
    return fim - inicio


# COMPARAÇÃO DE DESEMPENHO

#Exibe comparação de desempenho entre versões serial e paralela.
def comparar_tempos(nome_func, tempo_serial, tempo_paralelo):
    print("\n" + "=" * 60)
    print(f"  COMPARAÇÃO DE DESEMPENHO - {nome_func}")
    print("=" * 60)
    print(f"  Tempo Serial:   {tempo_serial:.4f}s")
    print(f"  Tempo Paralelo: {tempo_paralelo:.4f}s")
    if tempo_paralelo > 0:
        speedup = tempo_serial / tempo_paralelo
        print(f"  Speedup:        {speedup:.2f}x")
        if speedup > 1:
            print(f"  → Paralelo foi {speedup:.2f}x mais rápido.")
        else:
            print(f"  → Serial foi mais eficiente (overhead de paralelismo).")
    print("=" * 60 + "\n")



# INTERFACE DO USUÁRIO
def menu():
    print("\n" + "=" * 60)
    print("   SISTEMA DE MANIPULAÇÃO DE DADOS - JUSTIÇA ELEITORAL")
    print("=" * 60)
    print("  1. Concatenar todos os arquivos CSV")
    print("  2. Gerar resumo por município")
    print("  3. Top 10 tribunais por Meta1")
    print("  4. Filtrar por município")
    print("  0. Sair")
    print("=" * 60)
    return input("  Escolha uma opção: ").strip()


def carregar_base(diretorio="."):
    arquivos = encontrar_arquivos_csv(diretorio)
    dados = []
    for arq in arquivos:
        dados.extend(ler_csv(arq))
    print(f"\nBase carregada: {len(dados)} registros de {len(arquivos)} arquivo(s).")
    return dados


def main():
    diretorio = input("Diretório com os arquivos CSV (Enter para diretório atual): ").strip()
    if not diretorio:
        diretorio = "."

    while True:
        opcao = menu()

        if opcao == "0":
            print("Encerrando...")
            break

        elif opcao == "1":
            print("\n[Funcionalidade 1] Concatenando arquivos CSV...\n")
            t_serial = func1_serial(diretorio)
            t_paralelo = func1_paralela(diretorio)
            comparar_tempos("Func1 - Concatenação", t_serial, t_paralelo)

        elif opcao == "2":
            print("\n[Funcionalidade 2] Gerando resumo por município...\n")
            dados = carregar_base(diretorio)
            if dados:
                t_serial = func2_serial(dados)
                t_paralelo = func2_paralela(dados)
                comparar_tempos("Func2 - Resumo por Município", t_serial, t_paralelo)

        elif opcao == "3":
            print("\n[Funcionalidade 3] Gerando top 10 tribunais por Meta1...\n")
            dados = carregar_base(diretorio)
            if dados:
                t_serial = func3_serial(dados)
                t_paralelo = func3_paralela(dados)
                comparar_tempos("Func3 - Top 10 Tribunais", t_serial, t_paralelo)

        elif opcao == "4":
            municipio = input("Informe o município: ").strip()
            if not municipio:
                print("Município não informado.")
                continue
            print(f"\n[Funcionalidade 4] Filtrando por município: {municipio.upper()}\n")
            dados = carregar_base(diretorio)
            if dados:
                t_serial = func4_serial(dados, municipio)
                t_paralelo = func4_paralela(dados, municipio)
                comparar_tempos("Func4 - Filtro por Município", t_serial, t_paralelo)

        else:
            print("Opção inválida. Tente novamente.")


if __name__ == "__main__":
    main()
