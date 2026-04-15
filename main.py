import os, csv, glob, time, multiprocessing, copy
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ───────── UTIL ─────────
def csv_files(dir):
    return glob.glob(os.path.join(dir, "**", "*.csv"), recursive=True)

def read_csv(fp):
    with open(fp, encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        return r.fieldnames, list(r)

def write_csv(fp, header, rows):
    if not rows:
        print("Sem dados.")
        return
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    with open(fp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def write_txt(fp, header, rows):
    with open(fp, "w", encoding="utf-8") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(str(r.get(c,"")) for c in header) + "\n")

def f(x):
    try: return float(str(x).replace(",", "."))
    except: return 0.0

# ───────── METAS (com + conforme PDF) ─────────
def meta1(a,b,c,d): return (a/(b+c+d)*100) if b+c+d else 0
def m2a(a,b,c): return (a/(b+c)*(1000/7)) if b+c else 0
def m2ant(a,b,c,d): return (a/(b+c+d)*100) if b+c+d else 0
def m4a(a,b,c): return (a/(b+c)*100) if b+c else 0
def m4b(a,b,c): return (a/(b+c)*100) if b+c else 0

# ───────── CONCAT ─────────
def concat_serial(dir):
    files = csv_files(dir)
    if not files:
        raise Exception("Nenhum CSV encontrado")

    header, all_rows = None, []
    for fp in files:
        h, r = read_csv(fp)
        header = header or h
        all_rows.extend(r)

    return header, all_rows

def concat_paralelo(dir):
    files = csv_files(dir)
    with ProcessPoolExecutor() as ex:
        res = list(ex.map(read_csv, files))
    header = res[0][0]
    rows = [row for _,lst in res for row in lst]
    return header, rows

# ───────── RESUMO ─────────
def resumo(rows, chave):
    agg = defaultdict(lambda: defaultdict(float))

    for r in rows:
        k = r.get(chave,"").strip()
        for c in [
            "julgados_2026","casos_novos_2026","dessobrestados_2026","suspensos_2026",
            "julgm2_a","distm2_a","suspm2_a",
            "julgm2_ant","distm2_ant","suspm2_ant","desom2_ant",
            "julgm4_a","distm4_a","suspm4_a",
            "julgm4_b","distm4_b","suspm4_b"
        ]:
            agg[k][c] += f(r.get(c))

    out=[]
    for k,v in agg.items():
        out.append({
            chave:k,
            "total_julgados_2026": round(v["julgados_2026"],2),
            "Meta1": round(meta1(v["julgados_2026"],v["casos_novos_2026"],v["dessobrestados_2026"],v["suspensos_2026"]),2),
            "Meta2A": round(m2a(v["julgm2_a"],v["distm2_a"],v["suspm2_a"]),2),
            "Meta2Ant": round(m2ant(v["julgm2_ant"],v["distm2_ant"],v["suspm2_ant"],v["desom2_ant"]),2),
            "Meta4A": round(m4a(v["julgm4_a"],v["distm4_a"],v["suspm4_a"]),2),
            "Meta4B": round(m4b(v["julgm4_b"],v["distm4_b"],v["suspm4_b"]),2),
        })
    return out

# ───────── TOP 10 ─────────
def top10(rows):
    data = resumo(rows,"sigla_tribunal")
    return sorted(data, key=lambda x: x["Meta1"], reverse=True)[:10]

# ───────── FILTRO ─────────
def filtrar(rows, municipio):
    m = municipio.upper()
    return [r for r in rows if r.get("municipio_oj","").upper()==m]

# ───────── BENCHMARK ─────────
def bench(s,p):
    t=time.perf_counter(); rs=s(); ts=time.perf_counter()-t
    t=time.perf_counter(); rp=p(); tp=time.perf_counter()-t
    print(f"Serial: {ts:.3f}s | Paralelo: {tp:.3f}s | Speedup: {ts/tp:.2f}x")
    return rp

# ───────── MAIN ─────────
def main():
    dir = input("Diretório (Enter=atual): ").strip() or BASE_DIR
    out = os.path.join(dir,"saida")
    os.makedirs(out,exist_ok=True)

    print("\nCarregando dados...")
    try:
        header, rows = concat_serial(dir)
        rows_base = copy.deepcopy(rows)  # evita bug de mutação
        print(f"{len(rows)} linhas carregadas.")
    except Exception as e:
        print("Erro:", e)
        return

    while True:
        print("\n1) Concatenar\n2) Resumo município\n3) Top10\n4) Filtrar\n0) Sair")
        op = input("Escolha: ").strip()

        if op == "0":
            print("Encerrando...")
            break

        elif op == "1":
            h,r = bench(lambda: concat_serial(dir),
                        lambda: concat_paralelo(dir))
            write_csv(os.path.join(out,"concat.csv"), h, r)

        elif op == "2":
            r = resumo(rows_base,"municipio_oj")
            write_csv(os.path.join(out,"resumo.csv"), r[0].keys(), r)

        elif op == "3":
            r = top10(rows_base)
            write_csv(os.path.join(out,"top10.csv"), r[0].keys(), r)

        elif op == "4":
            m = input("Município: ").strip()
            if not m:
                print("Inválido.")
                continue
            r = filtrar(rows_base,m)
            write_txt(os.path.join(out,f"{m.upper()}.txt"), header, r)

        else:
            print("Opção inválida.")

        input("\nPressione Enter para continuar...")

if __name__ == "__main__":
    main()