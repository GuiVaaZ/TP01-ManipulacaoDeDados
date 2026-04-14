# TP01 – Manipulando Arquivos CSV
**Disciplina:** Programação Concorrente e Distribuída  
**Instituição:** Universidade Católica de Brasília – UCB  
**Período:** 1° Semestre de 2026

---

## 1. Estrutura do Projeto

```
tp01_manipulando_arquivos.py   ← código-fonte único
saida/                         ← arquivos gerados (criada automaticamente)
  concatenado.csv
  resumo_municipio.csv
  top10_tribunais.csv
  <MUNICIPIO>.txt
```

---

## 2. Como Executar

### Pré-requisitos
- Python 3.8 ou superior
- Sem dependências externas (somente biblioteca padrão)

### Execução
```bash
python tp01_manipulando_arquivos.py
```

Ao iniciar, o programa solicita o diretório onde estão os arquivos `.csv` da base de dados.  
Deixe em branco e pressione Enter para usar a pasta onde o script está localizado.

---

## 3. Funcionalidades

### Funcionalidade 1 – Concatenar arquivos CSV
Lê todos os arquivos `.csv` presentes no diretório informado e os combina em um único arquivo `concatenado.csv`.

- **Serial:** lê cada arquivo sequencialmente em um laço `for`.
- **Paralelo:** usa `ProcessPoolExecutor` para ler múltiplos arquivos simultaneamente, um processo por arquivo (limitado ao número de CPUs disponíveis).

---

### Funcionalidade 2 – Resumo por município
Agrupa todas as linhas por `municipio_oj` e calcula as seguintes métricas:

| Métrica | Fórmula |
|---|---|
| total_julgados_2026 | Σ julgados_2026 |
| Meta1 | (Σjulgados / (Σcasos_novos + Σdessobrestados + Σsuspensos)) × 100 |
| Meta2A | (Σjulgm2_a / (Σdistm2_a + Σsuspm2_a)) × (1000/7) |
| Meta2Ant | (Σjulgm2_ant / (Σdistm2_ant + Σsuspm2_ant + Σdesom2_ant)) × 100 |
| Meta4A | (Σjulgm4_a / (Σdistm4_a + Σsuspm4_a)) × 100 |
| Meta4B | (Σjulgm4_b / (Σdistm4_b + Σsuspm4_b)) × 100 |

> **Nota:** conforme especificado, todas as subtrações nas fórmulas originais foram convertidas em adições.

- **Serial:** itera sobre todas as linhas acumulando somas em um `defaultdict`.
- **Paralelo:** divide as linhas em N chunks (N = número de CPUs), agrega cada chunk em um processo separado e mescla os resultados com `functools.reduce`.

---

### Funcionalidade 3 – Top 10 tribunais por Meta1
Mesma lógica de agregação da Funcionalidade 2, mas agrupando por `sigla_tribunal`. Após calcular as métricas de todos os tribunais, ordena por `Meta1` em ordem decrescente e retorna os 10 primeiros.

- **Serial / Paralelo:** mesma estratégia de chunks descrita na Funcionalidade 2.

---

### Funcionalidade 4 – Filtrar por município → TXT
O usuário informa um nome de município (sem distinção de maiúsculas/minúsculas). O programa filtra todas as linhas cuja coluna `municipio_oj` corresponda ao valor informado e salva em `<MUNICIPIO>.txt` com campos separados por vírgula.

- **Serial:** itera linearmente sobre todas as linhas aplicando o filtro.
- **Paralelo:** divide as linhas em chunks, cada processo filtra o seu chunk; os resultados são concatenados ao final.

---

## 4. Análise de Desempenho

Ao executar cada funcionalidade, o programa imprime automaticamente:

```
  Serial  : X.XXXXs
  Paralelo: X.XXXXs
  Speedup : X.XXx
```

### Speedup esperado
O speedup (S = T_serial / T_paralelo) depende do número de núcleos disponíveis e do volume de dados. Em máquinas com 4+ núcleos e bases de dados grandes, é comum observar speedups de **2×–3×** para operações de leitura e agregação.

### Limitações da versão paralela
| Situação | Impacto |
|---|---|
| Base de dados pequena | Overhead de criação de processos supera o ganho |
| Máquina com poucos núcleos | Ganho limitado |
| Operações de I/O (disco) | Gargalo de I/O pode anular o paralelismo |

### Prós e Contras

| | Serial | Paralela |
|---|---|---|
| **Prós** | Simples, previsível, sem overhead | Menor tempo em bases grandes |
| **Contras** | Lento para grandes volumes | Overhead de processos, maior consumo de memória |

---

## 5. Decisões de Implementação

- **`ProcessPoolExecutor`** foi preferido sobre `ThreadPoolExecutor` pois o Python possui o GIL (Global Interpreter Lock), que impede paralelismo real em threads para código CPU-bound.
- **`defaultdict(lambda: defaultdict(float))`** foi usado para acumulação eficiente sem verificações `if key in dict`.
- A função `safe_float` trata campos vazios ou malformados convertendo-os para `0.0`, evitando erros de parsing.
- A escrita de TXT (Funcionalidade 4) usa vírgula como separador, mantendo compatibilidade com o formato CSV original, mas com extensão `.txt` conforme solicitado.

---

## 6. Estrutura do Código

```
tp01_manipulando_arquivos.py
│
├── Utilitários
│   ├── find_csv_files()
│   ├── safe_float()
│   ├── read_csv()
│   ├── write_csv()
│   └── write_txt()
│
├── Cálculos de Metas
│   ├── calc_meta1()
│   ├── calc_meta2a()
│   ├── calc_meta2ant()
│   ├── calc_meta4a()
│   └── calc_meta4b()
│
├── Funcionalidade 1
│   ├── concatenar_serial()
│   └── concatenar_paralelo()
│
├── Funcionalidade 2
│   ├── resumo_municipio_serial()
│   └── resumo_municipio_paralelo()
│
├── Funcionalidade 3
│   ├── top10_tribunais_serial()
│   └── top10_tribunais_paralelo()
│
├── Funcionalidade 4
│   ├── filtrar_municipio_serial()
│   └── filtrar_municipio_paralelo()
│
├── benchmark()          ← mede e imprime tempos + speedup
└── main()               ← menu interativo
```
