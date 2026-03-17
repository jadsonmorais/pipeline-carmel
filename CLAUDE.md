# CLAUDE.md — Guia para Agentes IA

Este arquivo define como agentes IA devem pensar e agir ao trabalhar neste projeto.
Leia antes de qualquer tarefa.

---

## REGRA OBRIGATÓRIA — Manutenção de Documentação

**Após qualquer alteração ao projeto** (novo ETL, nova tabela, nova view, novo módulo, nova dependência, nova variável de ambiente), você **DEVE** atualizar os três arquivos abaixo antes de considerar a tarefa concluída:

| Arquivo | O que atualizar |
|---------|----------------|
| `CLAUDE.md` | Contexto de negócio, campos críticos, skills, FAQ |
| `README.md` | Diagrama de arquitetura, estrutura de pastas, tabelas/views, comandos, .env |
| `db/build.sql` | Toda DDL nova (tabelas e views) — é a fonte da verdade do schema |
| `skills/<fonte>_etl.md` | Novo ETL ou novos comandos: estrutura de módulos, fluxos, comandos, variáveis |
| `skills/<fonte>_db.md` | Nova tabela ou view: schema, JSONB paths, queries úteis, chaves de conciliação |

Nunca finalize uma sessão de trabalho sem ter feito essas atualizações.

---

## Contexto de Negócio

Este é o **hub de dados operacionais da rede Carmel Hotéis** — não apenas um ETL de manutenção.

A visão é que **tudo está conectado**: um chamado de manutenção pode explicar um incidente que afetou a experiência de um hóspede, que aparece nos dados de reservas e marketing. O banco de dados unificado (`schema: carmel`) é o ponto de integração.

**Hotéis da rede**: CUMBUCO, TAÍBA, CHARME, MAGNA

**Fontes de dados integradas**:
- **Infraspeak** — manutenção corretiva e preventiva (via API HTTP)
- **PDV Simphony** — notas fiscais emitidas por ponto de venda (via SFTP, arquivos JSON diários)
- **NF-e XMLs (SMB)** — XMLs enviados ao fiscal, lidos dos shares `\\10.197.0.51\{Hotel}` (implementado)
- **FISCAL (CMERP)** — lançamentos fiscais por empresa/hotel (via API HTTP, `idExportacao=80`, implementado)
- **SEFAZ** — XMLs de NF-e/NFC-e autorizadas (via share de rede `\\10.197.1.3\Arquivos$\Sefaz` — em implementação)

**Banco de produção**: PostgreSQL em `10.197.3.2`, schema `carmel`

---

## Arquitetura — Entenda Antes de Modificar

```
shared/db.py         ← ÚNICO lugar para funções de banco
etls/<fonte>/        ← cada fonte de dados tem sua pasta isolada
  api.py             ← cliente HTTP específico da fonte
  sftp.py            ← cliente SFTP (fontes baseadas em arquivo, ex: PDV)
  parser.py          ← parser de arquivo (JSON, XML, etc.)
  extractor.py       ← lógica de extração com retry
  sync.py            ← orquestrador (entry point)
skills/              ← documentação de conhecimento para agentes IA
db/build.sql         ← DDL completo do banco (fonte da verdade)
```

**Nunca** crie arquivos Python na raiz do projeto. A raiz é apenas para configuração (`auto.bat`, `requirements.txt`, `.gitignore`, `CLAUDE.md`, `README.md`).

---

## Regras de Código

### Banco de Dados
- **Sempre upsert** — nunca `INSERT` direto. Usar `shared.db.upsert_raw_data()`
- **JSONB intacto nas tabelas raw** — nunca transformar, normalizar ou extrair campos nas tabelas raw. Toda lógica de negócio fica em views SQL
- **Novas tabelas** vão em `db/build.sql` no schema `carmel`
- **Novas views** também vão em `db/build.sql` — é a fonte da verdade do schema

### Imports
Dentro de `etls/<fonte>/`, usar sempre:
```python
from . import api, extractor          # imports internos do mesmo ETL
from shared import db as utils        # funções de banco
```
Nunca importar `utils` (arquivo legado deletado). Nunca duplicar funções de `shared/db.py`.

### Execução
Sempre como módulo Python a partir da raiz:
```bash
python -m etls.infraspeak.sync
python -m etls.pdv.sync
python -m etls.pdv.sync 2026-02-19   # data específica
```

---

## Campos Críticos — Armadilhas Conhecidas

### state vs status em Failures (Infraspeak)

| Tabela | Campo correto | Path JSONB |
|--------|--------------|------------|
| `infraspeak_raw_failures` (bulk) | `state` | `data -> 'attributes' ->> 'state'` |
| `infraspeak_raw_failure_details` | `status` | `data -> 'data' -> 'attributes' ->> 'status'` |

A tabela de detalhes tem um nível extra `data -> 'data'` porque armazena o envelope completo da API.

### EXCLUIDO — Valor Customizado do ETL
`EXCLUIDO` **não existe na API Infraspeak**. É um valor inserido pelo ETL quando um registro retorna 404 (foi deletado no ERP). Ao analisar dados, filtrar `state != 'EXCLUIDO'` para excluir registros deletados.

### Atualização de JSONB
Para atualizar um campo dentro do JSONB sem sobrescrever o objeto inteiro, usar `jsonb_set`:
```sql
jsonb_set(data, '{attributes,state}', '"NOVO_VALOR"')
```

### PDV Simphony — Estrutura do JSON

Arquivo diário por hotel. Nome: `{Hotel}CFB_{YYYY-MM-DD}.json`

```
[ [header FIS], [{}], [registros FISID...], [{}] ]
```

| Seção | Conteúdo |
|-------|----------|
| `data[0][0]` | Header: `Store Number`, `Store Name`, `First Business Date` |
| `data[2]` | Lista de registros `FISID` (um por nota fiscal) |

**Mapeamento Store Number → Hotel:**

| Store Number | Hotel canônico |
|-------------|---------------|
| `CUMBUCO` | CUMBUCO |
| `TAIBA` | TAIBA |
| `CARM` | CHARME |
| `MAGN` | MAGNA |

**Campo-chave**: `Invoice Data Info 8` = chave NF-e 44 dígitos = `nota_id` na tabela raw = chave de conciliação com NF-e XMLs e SEFAZ.

**Campos financeiros FISID** (semântica Simphony):
- `Sub Total 1` — valor bruto total da nota
- `Sub Total 2` — subtotal tributável (ISS)
- `Sub Total 3` — subtotal não tributável
- `Sub Total 6` — valor de consumação/gorjeta (depende da configuração do centro de receita)
- `Tax Total 1` — ISS calculado
- `Invoice Data Info 5` — número do quarto do hóspede
- `Invoice Data Info 6` — nome do garçom/operador

### NF-e XMLs (SMB) — Shares e Mapeamento

XMLs de NF-e/NFC-e emitidos pelo PDV e enviados ao fiscal. Lidos via protocolo SMB.

| Share                      | Hotel canônico |
|----------------------------|---------------|
| `\\10.197.0.51\Cumbuco`    | CUMBUCO       |
| `\\10.197.0.51\Charme`     | CHARME        |
| `\\10.197.0.51\Magna2`     | MAGNA         |
| `\\10.197.0.51\Taiba`      | TAIBA         |

**Convenção de nome de arquivo**:
- `NFe{44_digitos}-nfe.xml` — NF-e/NFC-e (nota fiscal) → tabela `nfe_raw_xmls`
- `NFe{44_digitos}-can.xml` — cancelamento NF-e → tabela `nfe_raw_cancelamentos`

**Chave (nfe_raw_xmls)**: 44 dígitos extraídos do atributo `infNFe/@Id` (strip do prefixo "NFe") = `nota_id` = chave de conciliação com `pdv_raw_notas`.

**Chave (nfe_raw_cancelamentos)**: `infEvento/@Id` = `cancelamento_id` (PK). O campo `data->>'chNFe'` aponta para `nfe_raw_xmls.nota_id`.

**Variáveis de ambiente**: `NFE_SMB_HOST`, `NFE_SMB_USER`, `NFE_SMB_PASS`, `NFE_SMB_DOMAIN`

### Fiscal CMERP — API de Lançamentos

API HTTP que retorna lançamentos fiscais por empresa/hotel via export parametrizado.

**Endpoint**: `GET https://carmel-api.cmerp.com.br/Global.API/ExportarDados/Executar?idExportacao=80&...`

**Parâmetros obrigatórios**: `DataIni`, `DataFim` (DateTime), `IdEmpresa` (string com IDs separados por vírgula)

**Identificação do hotel**: campo `FKEMPRESA` / `EMPRESA` no retorno.

**PK**: `IDLANCAMENTOICMSBASE` → `lancamento_id` na tabela `fiscal_raw_lancamentos`.

**Variáveis de ambiente**: `CLIENT-ID`, `CLIENT-SECRET`, `EMPRESA-IDS`, `USUARIO-ID`

---

## Padrões a Seguir

### Novo ETL (nova fonte de dados)
1. `etls/<fonte>/__init__.py` (vazio)
2. `etls/<fonte>/api.py` ou `sftp.py` com cliente da fonte
3. `etls/<fonte>/parser.py` se houver parse de arquivo
4. `etls/<fonte>/sync.py` importando `shared.db`
5. `etls/<fonte>/history_sync.py` para backfill por intervalo de datas
6. Tabelas e views em `db/build.sql`
7. `skills/<fonte>_etl.md` — módulos, fluxos, comandos, variáveis de ambiente
8. `skills/<fonte>_db.md` — tabelas, JSONB paths, views, queries úteis
9. Atualizar `CLAUDE.md` e `README.md` com a nova fonte

### ETL baseado em SFTP (padrão PDV)
```python
from . import sftp as sftp_module, parser
from shared import db as utils

def run(date_str=None):
    with sftp_module.SFTPClient() as client:
        files = client.list_files_for_date(date_str)
        for filename in files:
            content = client.download_content(filename)
            records = parser.parse_file(content, filename)
            utils.upsert_raw_data('tabela_raw', 'tipo_id', records, 'tipo')
```

### Tratamento de 404 em novas ETLs
Seguir o padrão já implementado para failures:
```python
if '404' in str(e):
    # marcar como deletado/inativo no banco
```

### Retry com backoff
Padrão: 3 tentativas, backoff exponencial (2s → 4s), falha definitiva → log + ação corretiva.

---

## O que NÃO Fazer

- Não criar arquivos Python na raiz do projeto
- Não duplicar `get_db_connection()` ou `upsert_raw_data()` fora de `shared/db.py`
- Não transformar dados nas tabelas raw — elas são arquivos históricos imutáveis
- Não commitar `auth/prod/.env` ou qualquer credencial
- Não usar `import utils` (arquivo legado removido) — usar `from shared import db as utils`
- Não rodar scripts diretamente com `python sync.py` — usar `python -m etls.<fonte>.sync`
- Não hardcodar caminhos como `H:/Meu Drive` — usar `Path(__file__).parent`
- **Não finalizar uma tarefa sem atualizar CLAUDE.md, README.md, db/build.sql e skills/**

---

## Skills Disponíveis

Para tarefas específicas, consulte os documentos em `skills/`:

| Skill | Quando usar |
|-------|-------------|
| `skills/infraspeak_db.md` | Escrever queries, entender schema Infraspeak, modificar banco |
| `skills/infraspeak_api.md` | Construir requisições, entender endpoints e filtros |
| `skills/infraspeak_etl.md` | Entender fluxos, criar novos ETLs, debugar extrações |
| `skills/pdv_db.md` | Escrever queries, entender JSONB do PDV, conciliação fiscal |
| `skills/pdv_etl.md` | Entender estrutura JSON do Simphony, SFTP, comandos PDV |
| `skills/nfe_db.md` | Escrever queries sobre NF-e XMLs, conciliação PDV↔NF-e |
| `skills/nfe_etl.md` | Entender fluxo SMB, shares por hotel, comandos NF-e |

---

## Perguntas Frequentes

**P: Onde fica a lógica de calcular horas de um operador?**
R: Em views SQL — `v_trabalho_analitico_operador_chamados` e `v_trabalho_analitico_operador_ocorrencias`. Não no Python.

**P: Por que um failure aparece com state EXCLUIDO mas ainda tem dados?**
R: Foi deletado no ERP após ter sido extraído. O ETL preserva o histórico e apenas muda o state para sinalizar que não existe mais na API.

**P: Como adicionar dados de reservas ao mesmo banco?**
R: Criar `etls/reservas/` com `api.py` e `sync.py`, adicionar tabelas em `db/build.sql`, e usar `shared.db.upsert_raw_data()` para persistir. As views podem então cruzar com dados de manutenção.

**P: Como executar um backfill de um período específico?**
R: `python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31 true`
O `true` final ativa a extração de event registries (necessário para cálculo de horas).

**P: Como sincronizar o PDV de uma data específica?**
R: `python -m etls.pdv.sync 2026-02-19`
Sem argumento, sincroniza o dia anterior (padrão de execução diária).

**P: Como fazer backfill do PDV em um intervalo de datas?**
R: `python -m etls.pdv.history_sync 2026-02-01 2026-02-28`
Abre uma única conexão SFTP e itera dia a dia. Erros em dias individuais não param o processo. Ao final, exibe total de notas e dias sem arquivo.

**P: Qual é a chave de conciliação entre PDV, NF-e e SEFAZ?**
R: `Invoice Data Info 8` no PDV = `infNFe/@Id` (strip "NFe") nos XMLs = chave NF-e 44 dígitos = `nota_id` nas tabelas `pdv_raw_notas` e `nfe_raw_xmls`. O join é imediato: `pdv_raw_notas.nota_id = nfe_raw_xmls.nota_id`.

**P: Como sincronizar os XMLs NF-e das pastas compartilhadas?**
R: `python -m etls.nfe.sync` — varre os 4 shares SMB (Cumbuco, Charme, Magna, Taiba) e persiste todos os XMLs encontrados. Idempotente: rodar novamente apenas atualiza `extracted_at`.
