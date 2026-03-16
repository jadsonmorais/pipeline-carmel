# CLAUDE.md — Guia para Agentes IA

Este arquivo define como agentes IA devem pensar e agir ao trabalhar neste projeto.
Leia antes de qualquer tarefa.

---

## Contexto de Negócio

Este é o **hub de dados operacionais da rede Carmel Hotéis** — não apenas um ETL de manutenção.

A visão é que **tudo está conectado**: um chamado de manutenção pode explicar um incidente que afetou a experiência de um hóspede, que aparece nos dados de reservas e marketing. O banco de dados unificado (`schema: carmel`) é o ponto de integração.

**Hotéis da rede**: CUMBUCO, TAÍBA, CHARME, MAGNA

**Fontes de dados integradas (atual e futuras)**:
- Infraspeak — manutenção corretiva e preventiva
- (futuras) reservas, marketing, RH, financeiro

**Banco de produção**: PostgreSQL em `10.197.3.2`, schema `carmel`

---

## Arquitetura — Entenda Antes de Modificar

```
shared/db.py         ← ÚNICO lugar para funções de banco
etls/<fonte>/        ← cada fonte de dados tem sua pasta isolada
  api.py             ← cliente HTTP específico da fonte
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
python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31 true
```

---

## Campos Críticos — Armadilhas Conhecidas

### state vs status em Failures

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

---

## Padrões a Seguir

### Novo ETL (nova fonte de dados)
1. `etls/<fonte>/__init__.py` (vazio)
2. `etls/<fonte>/api.py` com cliente HTTP
3. `etls/<fonte>/sync.py` importando `shared.db`
4. Tabelas em `db/build.sql`
5. Skill em `skills/<fonte>_db.md`, `skills/<fonte>_api.md`, `skills/<fonte>_etl.md`

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
- Não rodar scripts diretamente com `python sync.py` — usar `python -m etls.infraspeak.sync`
- Não hardcodar caminhos como `H:/Meu Drive` — usar `Path(__file__).parent`

---

## Skills Disponíveis

Para tarefas específicas, consulte os documentos em `skills/`:

| Skill | Quando usar |
|-------|-------------|
| `skills/infraspeak_db.md` | Escrever queries, entender schema, modificar banco |
| `skills/infraspeak_api.md` | Construir requisições, entender endpoints e filtros |
| `skills/infraspeak_etl.md` | Entender fluxos, criar novos ETLs, debugar extrações |

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
