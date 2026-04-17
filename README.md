# TP01 – Manipulação de Dados CSV com Paralelismo

> **Trabalho Prático 1 — Programação Concorrente e Distribuída**  
> Universidade Católica de Brasília (UCB) — 1º Semestre de 2026

---

## Sumário

1. [Sobre o Projeto](#sobre-o-projeto)
2. [Pré-requisitos](#pré-requisitos)
3. [Estrutura do Repositório](#estrutura-do-repositório)
4. [Como Executar](#como-executar)
5. [Campos da Base de Dados](#campos-da-base-de-dados)
6. [Cálculo das Metas](#cálculo-das-metas)
7. [Funcionalidades](#funcionalidades)
   - [Func1 – Concatenar Arquivos CSV](#func1--concatenar-arquivos-csv)
   - [Func2 – Resumo por Município](#func2--resumo-por-município)
   - [Func3 – Top 10 Tribunais por Meta1](#func3--top-10-tribunais-por-meta1)
   - [Func4 – Filtrar por Município](#func4--filtrar-por-município)
8. [Comparação Serial vs Paralelo](#comparação-serial-vs-paralelo)
9. [Saída dos Arquivos](#saída-dos-arquivos)
10. [Utilitários Internos](#utilitários-internos)
11. [Considerações Finais](#considerações-finais)

---

## Sobre o Projeto

Este sistema foi desenvolvido em **Python puro** (sem dependências externas) para leitura, processamento e análise de dados da **Justiça Eleitoral** armazenados em arquivos `.csv`.

O objetivo central é comparar o desempenho entre abordagens **seriais** e **paralelas** para quatro funcionalidades de manipulação de dados. Cada funcionalidade executa automaticamente as duas versões, mede os tempos de execução, calcula o **speedup** e apresenta a comparação ao usuário.

---

## Pré-requisitos

- **Python 3.8** ou superior
- Nenhuma biblioteca externa — utiliza apenas a **biblioteca padrão do Python**:
  - `os`, `csv`, `time`, `glob` — utilitários de sistema e arquivos
  - `threading`, `multiprocessing` — primitivas de concorrência
  - `concurrent.futures` — interface de alto nível para threads e processos
  - `collections.defaultdict` — agrupamento eficiente de dados

---

## Estrutura do Repositório

```
TP01-ManipulacaoDeDados/
│
├── main.py                  # Código principal
├── Base-de-dados/           # Arquivos CSV de entrada
│   └── *.csv
├── saida/                   # Arquivos gerados pelo programa
│   ├── concatenado_serial.csv
│   ├── concatenado_paralelo.csv
│   ├── resumo_municipio_serial.csv
│   ├── resumo_municipio_paralelo.csv
│   ├── top10_tribunais_serial.csv
│   ├── top10_tribunais_paralelo.csv
│   └── <MUNICIPIO>.csv / <MUNICIPIO>_paralelo.csv
├── .gitignore
├── LICENSE
└── README.md
```

---

## Como Executar

**1. Clone o repositório:**
```bash
git clone https://github.com/GuiVaaZ/TP01-ManipulacaoDeDados.git
cd TP01-ManipulacaoDeDados
```

**2. Execute o programa:**
```bash
python main.py
```

**3. Informe o diretório com os arquivos CSV** (ou pressione `Enter` para usar a pasta atual — recomendado apontar para `Base-de-dados/`):
```
Diretório com os arquivos CSV (Enter para diretório atual): .\Base-de-dados
```

**4. Escolha a funcionalidade no menu interativo:**
```
============================================================
   SISTEMA DE MANIPULAÇÃO DE DADOS - JUSTIÇA ELEITORAL
============================================================
  1. Concatenar todos os arquivos CSV
  2. Gerar resumo por município
  3. Top 10 tribunais por Meta1
  4. Filtrar por município
  0. Sair
============================================================
```

Os resultados são salvos automaticamente na pasta `saida/`.

---

## Campos da Base de Dados

A base de dados contém registros por comarca/vara eleitoral. Os principais campos utilizados nos cálculos são:

| Campo | Descrição |
|---|---|
| `municipio_oj` | Nome do município da unidade judiciária |
| `sigla_tribunal` | Sigla do tribunal eleitoral (ex: TRE-DF) |
| `julgados_2026` | Processos julgados no corrente ano (2026) |
| `casos_novos_2026` | Novos processos distribuídos em 2026 |
| `dessobrestados_2026` | Processos retirados de sobrestamento em 2026 |
| `suspensos_2026` | Processos suspensos em 2026 |
| `julgm2_a` | Julgados para Meta2A (processos do ano corrente) |
| `distm2_a` | Distribuídos para Meta2A |
| `suspm2_a` | Suspensos para Meta2A |
| `julgm2_ant` | Julgados para Meta2Ant (processos de anos anteriores) |
| `distm2_ant` | Distribuídos para Meta2Ant |
| `suspm2_ant` | Suspensos para Meta2Ant |
| `desom2_ant` | Dessobrestados para Meta2Ant |
| `julgm4_a` | Julgados para Meta4A |
| `distm4_a` | Distribuídos para Meta4A |
| `suspm4_a` | Suspensos para Meta4A |
| `julgm4_b` | Julgados para Meta4B |
| `distm4_b` | Distribuídos para Meta4B |
| `suspm4_b` | Suspensos para Meta4B |

> **Nota:** Campos ausentes ou com valores inválidos são tratados como `0` pela função `safe_float()`.

---

## Cálculo das Metas

O sistema implementa cinco métricas de desempenho judicial, baseadas nos indicadores do **Conselho Nacional de Justiça (CNJ)**. Todos os cálculos agregam os valores de todas as linhas de um grupo (município ou tribunal) antes de aplicar a fórmula.

---

### Meta 1 — Taxa de Congestionamento Invertida

Mede a proporção de processos julgados em relação ao total de processos que deveriam ter sido resolvidos no ano.

```
Meta1 = (Σ julgados_2026 / (Σ casos_novos_2026 + Σ dessobrestados_2026 - Σ suspensos_2026)) × 100
```

**Componentes:**
- **Numerador:** Total de processos julgados no ano de 2026
- **Denominador:** Total de processos que estavam aptos a julgamento (casos novos + dessobrestados, excluindo os suspensos, pois estes não podem ser julgados)
- **Resultado:** Percentual. Um valor de `100` significa que todos os processos elegíveis foram julgados

**Exemplo:**
```
julgados_2026 = 850
casos_novos_2026 = 1000
dessobrestados_2026 = 100
suspensos_2026 = 50

Meta1 = (850 / (1000 + 100 - 50)) × 100
Meta1 = (850 / 1050) × 100
Meta1 = 80,95%
```

---

### Meta 2A — Julgamento de Processos do Ano Corrente

Mede o percentual de processos distribuídos no ano corrente que foram julgados, ponderado por um fator de conversão `(1000/7 ≈ 142,857)`.

```
Meta2A = (Σ julgm2_a / (Σ distm2_a - Σ suspm2_a)) × (1000 / 7)
```

**Componentes:**
- **Numerador:** Processos do tipo Meta2 julgados no ano corrente
- **Denominador:** Processos distribuídos para Meta2 no ano corrente, excluindo os suspensos
- **Fator `1000/7`:** Fator de ponderação definido pelo CNJ para normalizar a métrica
- **Resultado:** Valor normalizado. Quanto maior, melhor o desempenho no julgamento de processos recentes

**Exemplo:**
```
julgm2_a = 700
distm2_a = 1000
suspm2_a = 50

Meta2A = (700 / (1000 - 50)) × (1000/7)
Meta2A = (700 / 950) × 142,857
Meta2A = 0,7368 × 142,857
Meta2A ≈ 105,26
```

---

### Meta 2Ant — Julgamento de Processos de Anos Anteriores

Mede o percentual de processos de anos anteriores (acervo) que foram julgados no período.

```
Meta2Ant = (Σ julgm2_ant / (Σ distm2_ant - Σ suspm2_ant - Σ desom2_ant)) × 100
```

**Componentes:**
- **Numerador:** Processos de anos anteriores do tipo Meta2 que foram julgados
- **Denominador:** Acervo de processos de anos anteriores, descontando suspensos e dessobrestados
- **Resultado:** Percentual de redução do estoque de processos antigos

**Exemplo:**
```
julgm2_ant = 300
distm2_ant = 800
suspm2_ant = 80
desom2_ant = 20

Meta2Ant = (300 / (800 - 80 - 20)) × 100
Meta2Ant = (300 / 700) × 100
Meta2Ant ≈ 42,86%
```

---

### Meta 4A — Julgamento de Processos Prioritários (Grupo A)

Mede o percentual de julgamento de processos classificados como prioritários no grupo A.

```
Meta4A = (Σ julgm4_a / (Σ distm4_a - Σ suspm4_a)) × 100
```

**Componentes:**
- **Numerador:** Processos prioritários do grupo A que foram julgados
- **Denominador:** Total de processos prioritários do grupo A elegíveis (distribuídos menos suspensos)
- **Resultado:** Percentual de cumprimento da meta para o grupo A

---

### Meta 4B — Julgamento de Processos Prioritários (Grupo B)

Análoga à Meta 4A, mas para o grupo B de processos prioritários.

```
Meta4B = (Σ julgm4_b / (Σ distm4_b - Σ suspm4_b)) × 100
```

**Componentes:**
- **Numerador:** Processos prioritários do grupo B que foram julgados
- **Denominador:** Total de processos prioritários do grupo B elegíveis
- **Resultado:** Percentual de cumprimento da meta para o grupo B

---

### Proteção contra Divisão por Zero

Em todas as fórmulas, o denominador é verificado antes da divisão:

```python
return (numerador / denominador * fator) if denominador != 0 else 0.0
```

Se o denominador for zero (ex: nenhum processo distribuído), a meta retorna `0.0`.

---

## Funcionalidades

### Func1 – Concatenar Arquivos CSV

**Objetivo:** Unir todos os arquivos `.csv` encontrados no diretório informado em um único arquivo de saída.

#### Versão Serial (`func1_serial`)

```
Para cada arquivo CSV no diretório:
    Lê o arquivo linha por linha
    Adiciona as linhas à lista geral
Salva a lista completa em um único CSV
```

- **Tecnologia:** Processamento sequencial com `open()` e `csv.DictReader`
- **Saída:** `saida/concatenado_serial.csv`

#### Versão Paralela (`func1_paralela`)

```
Descobre todos os arquivos CSV
Cria um pool de threads (ThreadPoolExecutor)
Submete a leitura de cada arquivo como uma tarefa independente
Aguarda todas as tarefas terminarem
Consolida os resultados
```

- **Tecnologia:** `ThreadPoolExecutor` com `max_workers = min(n_arquivos, cpu_count × 2)`
- **Por que threads?** A leitura de arquivos é uma operação **I/O-bound** (limitada pela velocidade do disco). O GIL do Python não impede o paralelismo nesse caso, pois threads ficam bloqueadas aguardando o I/O, liberando o GIL para outras threads executarem
- **Saída:** `saida/concatenado_paralelo.csv`

#### Quando o Paralelo é Mais Rápido?

Com **muitos arquivos pequenos**, o overhead de criação das threads é compensado pelo ganho de leitura simultânea. Com **poucos arquivos grandes**, a diferença é menor.

---

### Func2 – Resumo por Município

**Objetivo:** Agrupar todos os registros por `municipio_oj` e calcular as cinco metas para cada município.

#### Versão Serial (`func2_serial`)

```
Agrupa todas as linhas por município (defaultdict)
Para cada município:
    Calcula Meta1, Meta2A, Meta2Ant, Meta4A, Meta4B
    Adiciona ao resumo
Salva o resumo em CSV
```

- **Saída:** `saida/resumo_municipio_serial.csv`

#### Versão Paralela (`func2_paralela`)

```
Agrupa todas as linhas por município (defaultdict) — ainda serial
Cria um pool de processos (ProcessPoolExecutor)
Distribui o cálculo de cada município entre os processos
Consolida os resultados
Salva o resumo em CSV
```

- **Tecnologia:** `ProcessPoolExecutor` com `max_workers = min(n_municipios, cpu_count)`
- **Por que processos?** O cálculo das metas é **CPU-bound** (matemática pura). O GIL impediria threads de executar em paralelo real. Processos têm espaço de memória próprio e bypass do GIL
- **Worker:** `_calcular_municipio(args)` — recebe `(municipio, lista_de_linhas)` e retorna um dicionário com as metas calculadas
- **Saída:** `saida/resumo_municipio_paralelo.csv`

---

### Func3 – Top 10 Tribunais por Meta1

**Objetivo:** Calcular as metas para cada tribunal (`sigla_tribunal`) e retornar os 10 com maior valor de `Meta1`.

#### Versão Serial (`func3_serial`)

```
Agrupa todas as linhas por sigla_tribunal
Para cada tribunal:
    Calcula as 5 metas
Ordena por Meta1 (decrescente)
Retorna os 10 primeiros
Salva em CSV
```

- **Saída:** `saida/top10_tribunais_serial.csv`

#### Versão Paralela (`func3_paralela`)

```
Agrupa todas as linhas por sigla_tribunal
Cria pool de processos
Distribui o cálculo de cada tribunal entre os processos
Ordena por Meta1 (decrescente) — ainda serial
Retorna os 10 primeiros
Salva em CSV
```

- **Tecnologia:** `ProcessPoolExecutor` — mesma justificativa da Func2
- **Worker:** `_calcular_tribunal(args)` — análogo ao `_calcular_municipio`
- **Nota:** A ordenação e o corte top-10 são feitos serialmente após a coleta dos resultados, pois dependem de todos os valores calculados
- **Saída:** `saida/top10_tribunais_paralelo.csv`

---

### Func4 – Filtrar por Município

**Objetivo:** Dado um nome de município informado pelo usuário, extrair todas as linhas correspondentes e salvar em um CSV separado.

#### Versão Serial (`func4_serial`)

```
Converte o nome do município para maiúsculas
Filtra a lista de dados linha por linha
Salva as linhas filtradas em CSV
```

- **Busca:** Case-insensitive (ignora maiúsculas/minúsculas)
- **Saída:** `saida/<MUNICIPIO>.csv`

#### Versão Paralela (`func4_paralela`)

```
Divide a lista de dados em chunks (um por núcleo de CPU)
Cria pool de threads
Cada thread filtra seu chunk simultaneamente
Consolida os resultados filtrados
Salva em CSV
```

- **Tecnologia:** `ThreadPoolExecutor` — o filtro é simples comparação de strings, mais próximo de I/O do que de CPU intensiva; threads são suficientes
- **Número de chunks:** `cpu_count()` (um chunk por núcleo)
- **Worker:** `_filtrar_chunk(args)` — recebe `(chunk, municipio_upper)` e retorna as linhas que correspondem
- **Saída:** `saida/<MUNICIPIO>_paralelo.csv`

---

## Comparação Serial vs Paralelo

Ao final de cada funcionalidade, o sistema exibe automaticamente a comparação:

```
============================================================
  COMPARAÇÃO DE DESEMPENHO - Func2 - Resumo por Município
============================================================
  Tempo Serial:   3.2041s
  Tempo Paralelo: 1.1853s
  Speedup:        2.70x
  → Paralelo foi 2.70x mais rápido.
============================================================
```

### Fórmula do Speedup

```
Speedup = Tempo Serial / Tempo Paralelo
```

- **Speedup > 1:** A versão paralela foi mais rápida
- **Speedup < 1:** A versão serial foi mais eficiente (overhead dominou)
- **Speedup = N (número de núcleos):** Eficiência perfeita (raramente atingida na prática)

### Fatores que Limitam o Speedup — Lei de Amdahl

Nem todo programa pode ser paralelizado indefinidamente. A Lei de Amdahl estabelece que:

```
Speedup máximo = 1 / (S + (1 - S) / N)
```

Onde `S` é a fração serial do programa e `N` é o número de processadores. No nosso código, partes como o agrupamento por `defaultdict` e a ordenação final são sequenciais, limitando o ganho real.

---

### Tabela Comparativa: Prós e Contras

| Critério | Versão Serial | Versão Paralela |
|---|---|---|
| **Complexidade do código** | ✅ Simples e legível | ⚠️ Mais complexo (workers, futures) |
| **Overhead de inicialização** | ✅ Nenhum | ❌ Criação de processos/threads tem custo |
| **Desempenho com dados pequenos** | ✅ Geralmente mais rápido | ❌ Overhead pode superar o ganho |
| **Desempenho com dados grandes** | ❌ Mais lento | ✅ Ganho real em bases volumosas |
| **Uso de CPU** | ❌ Usa apenas 1 núcleo | ✅ Usa múltiplos núcleos |
| **Previsibilidade** | ✅ Determinístico | ⚠️ Ordem dos resultados pode variar |
| **Depuração** | ✅ Fácil (stack trace linear) | ❌ Mais difícil (race conditions, deadlocks) |
| **Uso de memória** | ✅ Baixo | ❌ Processos duplicam a memória |
| **GIL do Python** | — | ⚠️ Threads: afetadas. Processos: bypass |

### Quando Usar Cada Abordagem?

**Prefira a versão serial quando:**
- A base de dados é pequena (poucos milhares de registros)
- O ambiente tem poucos núcleos de CPU
- A simplicidade de manutenção é prioritária
- O código será executado como parte de um pipeline maior

**Prefira a versão paralela quando:**
- A base de dados é grande (centenas de milhares ou mais de registros)
- O servidor tem muitos núcleos de CPU (4+)
- O tempo de processamento é crítico para o usuário
- As tarefas são independentes entre si (sem dependência de estado compartilhado)

---

## Saída dos Arquivos

Todos os arquivos são salvos na pasta `saida/`, criada automaticamente se não existir.

| Arquivo | Gerado por | Conteúdo |
|---|---|---|
| `concatenado_serial.csv` | Func1 Serial | Todos os registros unidos |
| `concatenado_paralelo.csv` | Func1 Paralela | Todos os registros unidos |
| `resumo_municipio_serial.csv` | Func2 Serial | Meta1–4B por município |
| `resumo_municipio_paralelo.csv` | Func2 Paralela | Meta1–4B por município |
| `top10_tribunais_serial.csv` | Func3 Serial | Top 10 tribunais por Meta1 |
| `top10_tribunais_paralelo.csv` | Func3 Paralela | Top 10 tribunais por Meta1 |
| `<MUNICIPIO>.csv` | Func4 Serial | Linhas filtradas do município |
| `<MUNICIPIO>_paralelo.csv` | Func4 Paralela | Linhas filtradas do município |

---

## Utilitários Internos

### `garantir_diretorio_saida()`
Cria a pasta `saida/` caso não exista, usando `os.makedirs(..., exist_ok=True)`.

### `encontrar_arquivos_csv(diretorio)`
Usa `glob.glob()` para localizar todos os arquivos `.csv` no diretório especificado.

### `ler_csv(caminho)`
Lê um arquivo CSV e retorna uma lista de dicionários (um por linha), usando `csv.DictReader`. Erros são capturados e exibidos sem interromper o programa.

### `salvar_csv(caminho, dados, campos)`
Salva uma lista de dicionários em um arquivo CSV dentro da pasta `saida/`, usando `csv.DictWriter`.

### `safe_float(valor)`
Converte um valor para `float` de forma segura:
- Substitui vírgulas por pontos (`1,5` → `1.5`)
- Remove espaços extras
- Retorna `0.0` em caso de erro (valor vazio, `None`, texto inválido)

### `calcular_metas_grupo(rows)`
Função central que chama todos os cinco cálculos de meta para um grupo de linhas e retorna um dicionário com os resultados arredondados a 4 casas decimais.

### `comparar_tempos(nome_func, tempo_serial, tempo_paralelo)`
Exibe no terminal a tabela de comparação de desempenho com tempo serial, tempo paralelo e speedup calculado.

### `carregar_base(diretorio)`
Carrega todos os CSVs do diretório e retorna uma lista unificada de registros, exibindo o total de linhas e arquivos lidos.

---

## Considerações Finais

- **GIL (Global Interpreter Lock):** O Python possui um mecanismo que impede que múltiplas threads executem bytecode Python simultaneamente. Por isso, operações **CPU-bound** (como cálculo das metas) utilizam `ProcessPoolExecutor`, enquanto operações **I/O-bound** (como leitura de arquivos) usam `ThreadPoolExecutor`.

- **Overhead de Processos:** Criar processos tem um custo inicial significativo (cópia de memória, inicialização). Em bases de dados pequenas, esse custo pode tornar a versão paralela mais lenta que a serial.

- **Consistência dos Resultados:** As versões serial e paralela produzem os mesmos resultados numéricos. A ordem das linhas no CSV de saída pode variar na versão paralela, pois as tarefas são concluídas em ordem não determinística.

- **Escalabilidade:** O sistema escala automaticamente com base no número de núcleos da CPU (`multiprocessing.cpu_count()`), aproveitando ao máximo o hardware disponível.

---

## Licença

Distribuído sob a licença **MIT**. Veja o arquivo `LICENSE` para mais detalhes.

---

*Desenvolvido para a disciplina de Programação Concorrente e Distribuída — UCB, 2026.*
