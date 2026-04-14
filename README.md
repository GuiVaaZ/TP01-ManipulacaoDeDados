# TP01 – Manipulando Arquivos CSV

> Trabalho Prático 1 — Programação Concorrente e Distribuída  
> Universidade Católica de Brasília (UCB) — 1° Semestre de 2026

---

## Sobre o projeto

Sistema em Python para leitura e manipulação de dados da Justiça Eleitoral em formato CSV. Cada funcionalidade possui **duas versões**: uma **serial** e uma **paralela**, com medição automática de tempo e speedup.

---

## Pré-requisitos

- Python **3.8** ou superior
- Nenhuma biblioteca externa — apenas a biblioteca padrão do Python

---

## Como executar

1. Coloque o arquivo `tp01_manipulando_arquivos.py` na mesma pasta que os arquivos `.csv` da base de dados (ou anote o caminho deles).

2. Execute:
   ```bash
   python tp01_manipulando_arquivos.py
   ```

3. Quando solicitado, informe o diretório onde estão os CSVs (ou pressione **Enter** para usar a pasta atual).

4. Escolha a funcionalidade desejada no menu interativo.

Os arquivos de saída são salvos automaticamente na subpasta `saida/`.

---

## Funcionalidades

### 1 — Concatenar arquivos CSV
Combina todos os arquivos `.csv` do diretório em um único arquivo.

**Saída:** `saida/concatenado.csv`

---

### 2 — Resumo por município
Agrupa os dados por `municipio_oj` e calcula as seguintes métricas para cada município:

| Coluna | Fórmula |
|---|---|
| `total_julgados_2026` | Σ julgados_2026 |
| `Meta1` | (Σ julgados / (Σ casos_novos + Σ dessobrestados + Σ suspensos)) × 100 |
| `Meta2A` | (Σ julgm2_a / (Σ distm2_a + Σ suspm2_a)) × (1000/7) |
| `Meta2Ant` | (Σ julgm2_ant / (Σ distm2_ant + Σ suspm2_ant + Σ desom2_ant)) × 100 |
| `Meta4A` | (Σ julgm4_a / (Σ distm4_a + Σ suspm4_a)) × 100 |
| `Meta4B` | (Σ julgm4_b / (Σ distm4_b + Σ suspm4_b)) × 100 |

> Os denominadores usam **adições** em todos os termos, conforme especificação do enunciado.

**Saída:** `saida/resumo_municipio.csv`

---

### 3 — Top 10 tribunais por Meta1
Calcula as métricas Meta1–Meta4B por `sigla_tribunal` e retorna os **10 tribunais com maior Meta1**, em ordem decrescente.

**Saída:** `saida/top10_tribunais.csv`

---

### 4 — Filtrar por município → TXT
O usuário informa um nome de município e o programa extrai todas as linhas correspondentes, salvando em um arquivo `.txt`.

**Exemplo:** informar `MACAPA` gera `saida/MACAPA.txt`

---

## Paralelismo

Todas as funcionalidades possuem versão serial e paralela. Ao executar qualquer opção, o programa roda **ambas as versões** e exibe:

```
  Serial  : 2.3041s
  Paralelo: 0.8817s
  Speedup : 2.61x
```

A versão paralela utiliza `ProcessPoolExecutor`, que cria processos reais e contorna o GIL (Global Interpreter Lock) do Python, permitindo paralelismo verdadeiro em operações CPU-bound.

| | Serial | Paralela |
|---|---|---|
| **Prós** | Simples, previsível, sem overhead | Menor tempo em bases grandes |
| **Contras** | Lento para grandes volumes | Overhead de criação de processos |

> Em bases de dados pequenas, a versão serial pode ser mais rápida devido ao overhead de criação dos processos.

---

## Estrutura de saída

```
saida/
├── concatenado.csv
├── resumo_municipio.csv
├── top10_tribunais.csv
└── <MUNICIPIO>.txt
```

---

## Estrutura do código

```
tp01_manipulando_arquivos.py
│
├── Utilitários            find_csv_files, safe_float, read_csv, write_csv, write_txt
├── Cálculos de metas      calc_meta1, calc_meta2a, calc_meta2ant, calc_meta4a, calc_meta4b
├── Funcionalidade 1       concatenar_serial / concatenar_paralelo
├── Funcionalidade 2       resumo_municipio_serial / resumo_municipio_paralelo
├── Funcionalidade 3       top10_tribunais_serial / top10_tribunais_paralelo
├── Funcionalidade 4       filtrar_municipio_serial / filtrar_municipio_paralelo
├── benchmark()            mede tempo serial vs paralelo e calcula speedup
└── main()                 menu interativo
```

---

## Observações

- Campos numéricos vazios ou malformados são tratados como `0` automaticamente.
- A busca por município na Funcionalidade 4 é **case-insensitive** (maiúsculas/minúsculas são ignoradas).
- O arquivo de saída da Funcionalidade 4 é `.txt` com campos separados por vírgula.
