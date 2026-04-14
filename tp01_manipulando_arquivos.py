"""
TP01 - Manipulando Arquivos CSV
Universidade Católica de Brasília – UCB
Programação Concorrente e Distribuída – 1° Semestre de 2026

Funcionalidades:
1) Concatenar arquivos CSV
2) Gerar resumo por município_oj
3) Top 10 tribunais por Meta1
4) Filtrar por município e gerar arquivo TXT

Cada funcionalidade possui versão serial e paralela.
"""

import os
import csv
import time
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import reduce
from collections import defaultdict
import multiprocessing

# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def find_csv_files(directory=None):
    """Retorna lista de todos os arquivos .csv no diretório informado."""
    if directory is None:
        directory = BASE_DIR
    pattern = os.path.join(directory, "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {directory}")
    return sorted(files)


def safe_float(value):
    """Converte para float com segurança; retorna 0.0 em caso de erro."""
    try:
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0


def read_csv(filepath):
    """Lê um arquivo CSV e retorna (header, linhas)."""
    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames
    return header, rows


def write_csv(filepath, header, rows):
    """Escreve linhas em um arquivo CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def write_txt(filepath, header, rows):
    """Escreve linhas em um arquivo TXT separado por vírgula."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            f.write(",".join(str(row.get(col, "")) for col in header) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CÁLCULOS DE METAS
# ─────────────────────────────────────────────────────────────────────────────

def calc_meta1(julgados, casos_novos, dessobrestados, suspensos):
    """
    Meta1 = (Σjulgados_2026 / (Σcasos_novos_2026 + Σdessobrestados_2026 + Σsuspensos_2026)) * 100
    Nota: conforme instrução, subtrações foram substituídas por adições.
    """
    den = casos_novos + dessobrestados + suspensos
    return (julgados / den * 100) if den != 0 else 0.0


def calc_meta2a(julgm2_a, distm2_a, suspm2_a):
    """
    Meta2A = (Σjulgm2_a / (Σdistm2_a + Σsuspm2_a)) * (1000/7)
    """
    den = distm2_a + suspm2_a
    return (julgm2_a / den * (1000 / 7)) if den != 0 else 0.0


def calc_meta2ant(julgm2_ant, distm2_ant, suspm2_ant, desom2_ant):
    """
    Meta2Ant = (Σjulgm2_ant / (Σdistm2_ant + Σsuspm2_ant + Σdesom2_ant)) * 100
    """
    den = distm2_ant + suspm2_ant + desom2_ant
    return (julgm2_ant / den * 100) if den != 0 else 0.0


def calc_meta4a(julgm4_a, distm4_a, suspm4_a):
    """
    Meta4A = (Σjulgm4_a / (Σdistm4_a + Σsuspm4_a)) * 100
    """
    den = distm4_a + suspm4_a
    return (julgm4_a / den * 100) if den != 0 else 0.0


def calc_meta4b(julgm4_b, distm4_b, suspm4_b):
    """
    Meta4B = (Σjulgm4_b / (Σdistm4_b + Σsuspm4_b)) * 100
    """
    den = distm4_b + suspm4_b
    return (julgm4_b / den * 100) if den != 0 else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONALIDADE 1 – CONCATENAR ARQUIVOS CSV
# ─────────────────────────────────────────────────────────────────────────────

def _read_file_rows(filepath):
    """Worker: lê um arquivo e retorna (header, rows)."""
    return read_csv(filepath)


def concatenar_serial(directory=None, output_file="concatenado.csv"):
    """
    [SERIAL] Concatena todos os CSVs do diretório em um único arquivo.
    """
    files = find_csv_files(directory)
    all_rows = []
    header = None

    for fp in files:
        h, rows = read_csv(fp)
        if header is None:
            header = h
        all_rows.extend(rows)

    write_csv(output_file, header, all_rows)
    return output_file, len(all_rows)


def concatenar_paralelo(directory=None, output_file="concatenado.csv"):
    """
    [PARALELO] Concatena todos os CSVs usando ProcessPoolExecutor.
    Cada processo lê um arquivo independentemente.
    """
    files = find_csv_files(directory)
    header = None
    all_rows = []

    workers = min(len(files), multiprocessing.cpu_count())
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_read_file_rows, fp): fp for fp in files}
        for future in as_completed(futures):
            h, rows = future.result()
            if header is None:
                header = h
            all_rows.extend(rows)

    write_csv(output_file, header, all_rows)
    return output_file, len(all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONALIDADE 2 – RESUMO POR MUNICÍPIO
# ─────────────────────────────────────────────────────────────────────────────

def _agregar_municipio_chunk(rows):
    """
    Worker: agrega somas dos campos numéricos por município em um chunk de linhas.
    Retorna dict[municipio] -> dict[campo] -> float
    """
    agg = defaultdict(lambda: defaultdict(float))
    campos = [
        "julgados_2026", "casos_novos_2026", "dessobrestados_2026", "suspensos_2026",
        "julgm2_a", "distm2_a", "suspm2_a",
        "julgm2_ant", "distm2_ant", "suspm2_ant", "desom2_ant",
        "julgm4_a", "distm4_a", "suspm4_a",
        "julgm4_b", "distm4_b", "suspm4_b",
    ]
    for row in rows:
        mun = row.get("municipio_oj", "").strip()
        for campo in campos:
            agg[mun][campo] += safe_float(row.get(campo, 0))
    return {k: dict(v) for k, v in agg.items()}


def _merge_agg(agg1, agg2):
    """Mescla dois dicionários de agregação."""
    result = defaultdict(lambda: defaultdict(float))
    for agg in (agg1, agg2):
        for mun, campos in agg.items():
            for campo, valor in campos.items():
                result[mun][campo] += valor
    return {k: dict(v) for k, v in result.items()}


def _build_resumo_rows(agg):
    """Converte dicionário agregado em lista de linhas para CSV."""
    header = ["municipio_oj", "total_julgados_2026", "Meta1", "Meta2A", "Meta2Ant", "Meta4A", "Meta4B"]
    rows = []
    for mun, v in sorted(agg.items()):
        rows.append({
            "municipio_oj": mun,
            "total_julgados_2026": round(v.get("julgados_2026", 0), 2),
            "Meta1": round(calc_meta1(
                v.get("julgados_2026", 0), v.get("casos_novos_2026", 0),
                v.get("dessobrestados_2026", 0), v.get("suspensos_2026", 0)), 2),
            "Meta2A": round(calc_meta2a(
                v.get("julgm2_a", 0), v.get("distm2_a", 0), v.get("suspm2_a", 0)), 2),
            "Meta2Ant": round(calc_meta2ant(
                v.get("julgm2_ant", 0), v.get("distm2_ant", 0),
                v.get("suspm2_ant", 0), v.get("desom2_ant", 0)), 2),
            "Meta4A": round(calc_meta4a(
                v.get("julgm4_a", 0), v.get("distm4_a", 0), v.get("suspm4_a", 0)), 2),
            "Meta4B": round(calc_meta4b(
                v.get("julgm4_b", 0), v.get("distm4_b", 0), v.get("suspm4_b", 0)), 2),
        })
    return header, rows


def resumo_municipio_serial(all_rows, output_file="resumo_municipio.csv"):
    """
    [SERIAL] Gera resumo por município com as medidas Meta1–Meta4B.
    """
    agg = _agregar_municipio_chunk(all_rows)
    header, rows = _build_resumo_rows(agg)
    write_csv(output_file, header, rows)
    return output_file, len(rows)


def resumo_municipio_paralelo(all_rows, output_file="resumo_municipio.csv"):
    """
    [PARALELO] Divide as linhas em chunks e agrega em paralelo, depois mescla.
    """
    n_workers = multiprocessing.cpu_count()
    chunk_size = max(1, len(all_rows) // n_workers)
    chunks = [all_rows[i:i + chunk_size] for i in range(0, len(all_rows), chunk_size)]

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        parciais = list(executor.map(_agregar_municipio_chunk, chunks))

    agg_final = reduce(_merge_agg, parciais)
    header, rows = _build_resumo_rows(agg_final)
    write_csv(output_file, header, rows)
    return output_file, len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONALIDADE 3 – TOP 10 TRIBUNAIS POR META1
# ─────────────────────────────────────────────────────────────────────────────

def _agregar_tribunal_chunk(rows):
    """Worker: agrega somas dos campos numéricos por sigla_tribunal."""
    agg = defaultdict(lambda: defaultdict(float))
    campos = [
        "julgados_2026", "casos_novos_2026", "dessobrestados_2026", "suspensos_2026",
        "julgm2_a", "distm2_a", "suspm2_a",
        "julgm2_ant", "distm2_ant", "suspm2_ant", "desom2_ant",
        "julgm4_a", "distm4_a", "suspm4_a",
        "julgm4_b", "distm4_b", "suspm4_b",
    ]
    for row in rows:
        trib = row.get("sigla_tribunal", "").strip()
        for campo in campos:
            agg[trib][campo] += safe_float(row.get(campo, 0))
    return {k: dict(v) for k, v in agg.items()}


def _merge_agg_tribunal(agg1, agg2):
    return _merge_agg(agg1, agg2)


def _build_top10_rows(agg):
    header = ["sigla_tribunal", "Meta1", "Meta2A", "Meta2Ant", "Meta4A", "Meta4B"]
    rows = []
    for trib, v in agg.items():
        rows.append({
            "sigla_tribunal": trib,
            "Meta1": round(calc_meta1(
                v.get("julgados_2026", 0), v.get("casos_novos_2026", 0),
                v.get("dessobrestados_2026", 0), v.get("suspensos_2026", 0)), 2),
            "Meta2A": round(calc_meta2a(
                v.get("julgm2_a", 0), v.get("distm2_a", 0), v.get("suspm2_a", 0)), 2),
            "Meta2Ant": round(calc_meta2ant(
                v.get("julgm2_ant", 0), v.get("distm2_ant", 0),
                v.get("suspm2_ant", 0), v.get("desom2_ant", 0)), 2),
            "Meta4A": round(calc_meta4a(
                v.get("julgm4_a", 0), v.get("distm4_a", 0), v.get("suspm4_a", 0)), 2),
            "Meta4B": round(calc_meta4b(
                v.get("julgm4_b", 0), v.get("distm4_b", 0), v.get("suspm4_b", 0)), 2),
        })
    rows.sort(key=lambda r: r["Meta1"], reverse=True)
    return header, rows[:10]


def top10_tribunais_serial(all_rows, output_file="top10_tribunais.csv"):
    """
    [SERIAL] Retorna os 10 tribunais com maior Meta1, ordenados decrescentemente.
    """
    agg = _agregar_tribunal_chunk(all_rows)
    header, rows = _build_top10_rows(agg)
    write_csv(output_file, header, rows)
    return output_file, len(rows)


def top10_tribunais_paralelo(all_rows, output_file="top10_tribunais.csv"):
    """
    [PARALELO] Agrega tribunais em chunks paralelos e seleciona top 10.
    """
    n_workers = multiprocessing.cpu_count()
    chunk_size = max(1, len(all_rows) // n_workers)
    chunks = [all_rows[i:i + chunk_size] for i in range(0, len(all_rows), chunk_size)]

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        parciais = list(executor.map(_agregar_tribunal_chunk, chunks))

    agg_final = reduce(_merge_agg_tribunal, parciais)
    header, rows = _build_top10_rows(agg_final)
    write_csv(output_file, header, rows)
    return output_file, len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONALIDADE 4 – FILTRAR POR MUNICÍPIO → ARQUIVO TXT
# ─────────────────────────────────────────────────────────────────────────────

def _filtrar_chunk(args):
    """Worker: filtra linhas de um chunk pelo município informado."""
    rows, municipio = args
    municipio_upper = municipio.strip().upper()
    return [r for r in rows if r.get("municipio_oj", "").strip().upper() == municipio_upper]


def filtrar_municipio_serial(all_rows, header, municipio, output_dir="."):
    """
    [SERIAL] Filtra todas as ocorrências do município e salva em <MUNICIPIO>.txt
    """
    municipio_upper = municipio.strip().upper()
    filtered = [r for r in all_rows if r.get("municipio_oj", "").strip().upper() == municipio_upper]
    output_file = os.path.join(output_dir, f"{municipio_upper}.txt")
    write_txt(output_file, header, filtered)
    return output_file, len(filtered)


def filtrar_municipio_paralelo(all_rows, header, municipio, output_dir="."):
    """
    [PARALELO] Filtra em paralelo por chunks e combina resultados.
    """
    n_workers = multiprocessing.cpu_count()
    chunk_size = max(1, len(all_rows) // n_workers)
    chunks = [(all_rows[i:i + chunk_size], municipio) for i in range(0, len(all_rows), chunk_size)]

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        parciais = list(executor.map(_filtrar_chunk, chunks))

    filtered = [row for parte in parciais for row in parte]
    municipio_upper = municipio.strip().upper()
    output_file = os.path.join(output_dir, f"{municipio_upper}.txt")
    write_txt(output_file, header, filtered)
    return output_file, len(filtered)


# ─────────────────────────────────────────────────────────────────────────────
# BENCHMARK – mede tempo e calcula speedup
# ─────────────────────────────────────────────────────────────────────────────

def benchmark(func_serial, func_paralelo, *args, **kwargs):
    """
    Executa versão serial e paralela, imprime tempos e speedup.
    Retorna resultado da versão paralela.
    """
    t0 = time.perf_counter()
    result_serial = func_serial(*args, **kwargs)
    t_serial = time.perf_counter() - t0

    t0 = time.perf_counter()
    result_paralelo = func_paralelo(*args, **kwargs)
    t_paralelo = time.perf_counter() - t0

    speedup = t_serial / t_paralelo if t_paralelo > 0 else float("inf")
    print(f"  Serial  : {t_serial:.4f}s")
    print(f"  Paralelo: {t_paralelo:.4f}s")
    print(f"  Speedup : {speedup:.2f}x")
    return result_serial, result_paralelo, t_serial, t_paralelo, speedup


# ─────────────────────────────────────────────────────────────────────────────
# INTERFACE DE MENU
# ─────────────────────────────────────────────────────────────────────────────

def menu():
    print("\n" + "=" * 60)
    print("  TP01 – Manipulando Arquivos CSV")
    print("  Programação Concorrente e Distribuída – UCB 2026")
    print("=" * 60)
    print("  1) Concatenar arquivos CSV")
    print("  2) Resumo por município (Meta1–Meta4B)")
    print("  3) Top 10 tribunais por Meta1")
    print("  4) Filtrar por município → arquivo TXT")
    print("  0) Sair")
    print("=" * 60)
    return input("  Escolha uma opção: ").strip()


def main():
    # Permite informar diretório da base de dados
    dir_input = input("Diretório dos arquivos CSV [Enter = pasta atual]: ").strip()
    base_dir = dir_input if dir_input else BASE_DIR
    out_dir = os.path.join(base_dir, "saida")
    os.makedirs(out_dir, exist_ok=True)

    # Pré-carrega todos os dados para Funcionalidades 2, 3 e 4
    print("\nCarregando dados...")
    try:
        files = find_csv_files(base_dir)
        all_rows = []
        header = None
        for fp in files:
            h, rows = read_csv(fp)
            if header is None:
                header = h
            all_rows.extend(rows)
        print(f"  {len(files)} arquivo(s) carregado(s) | {len(all_rows)} linha(s) no total.")
    except FileNotFoundError as e:
        print(f"ERRO: {e}")
        return

    while True:
        opcao = menu()

        if opcao == "0":
            print("Encerrando. Até logo!")
            break

        elif opcao == "1":
            print("\n[1] Concatenar arquivos CSV")
            out = os.path.join(out_dir, "concatenado.csv")
            result_s, result_p, ts, tp, sp = benchmark(
                lambda: concatenar_serial(base_dir, out),
                lambda: concatenar_paralelo(base_dir, out),
            )
            print(f"  Arquivo gerado: {out}  ({result_p[1]} linhas)")

        elif opcao == "2":
            print("\n[2] Resumo por município")
            out = os.path.join(out_dir, "resumo_municipio.csv")
            result_s, result_p, ts, tp, sp = benchmark(
                resumo_municipio_serial, resumo_municipio_paralelo,
                all_rows, out,
            )
            print(f"  Arquivo gerado: {out}  ({result_p[1]} municípios)")

        elif opcao == "3":
            print("\n[3] Top 10 tribunais por Meta1")
            out = os.path.join(out_dir, "top10_tribunais.csv")
            result_s, result_p, ts, tp, sp = benchmark(
                top10_tribunais_serial, top10_tribunais_paralelo,
                all_rows, out,
            )
            print(f"  Arquivo gerado: {out}  ({result_p[1]} tribunais)")

        elif opcao == "4":
            municipio = input("\n[4] Informe o nome do município (ex: MACAPA): ").strip()
            if not municipio:
                print("  Nome do município não pode ser vazio.")
                continue
            result_s, result_p, ts, tp, sp = benchmark(
                filtrar_municipio_serial, filtrar_municipio_paralelo,
                all_rows, header, municipio, out_dir,
            )
            print(f"  Arquivo gerado: {result_p[0]}  ({result_p[1]} ocorrências)")

        else:
            print("  Opção inválida. Tente novamente.")

        input("\n  Pressione Enter para continuar...")


if __name__ == "__main__":
    main()
