# Skill: Infraspeak — API v3

> Referência completa da API Infraspeak v3: autenticação, endpoints, filtros, expansões e limites.
> Use este documento para construir novas requisições ou debugar integrações.

---

## Configuração Base

- **Base URL**: `https://api.infraspeak.com/v3`
- **Autenticação**: Bearer Token (Personal Access Token)
- **Headers obrigatórios**:
  ```
  Authorization: Bearer {token}
  Accept: application/json
  Content-Type: application/json
  User-Agent: Infraspeak_V2_ETL ({user})
  ```
- **Rate limit**: 60 requisições/minuto
- **Credenciais**: `API_DATA_USER` e `API_DATA_TOKEN` em `auth/prod/.env`

---

## Rate Limiting (429)

Quando a API retorna HTTP 429, o header `Retry-After` indica quantos segundos aguardar:

```python
if response.status_code == 429:
    retry_after = int(response.headers.get("Retry-After", 60))
    time.sleep(retry_after)
    return self.request(endpoint, params)  # retry automático
```

Buffer padrão entre chamadas bem-sucedidas: `time.sleep(0.4)`

---

## Paginação

A API retorna no máximo 200 registros por página. O controle de paginação usa:
```json
{
  "meta": {
    "pagination": {
      "total_pages": 5,
      "total": 950,
      "per_page": 200,
      "current_page": 1
    }
  }
}
```

**Parâmetro de página**: `?page=N`

Uso no código: `api_client.request_all_pages(endpoint, params)` em `etls/infraspeak/api.py`

---

## Endpoints por Entidade

### Failures (Chamados Corretivos)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `failures` | Lista todos os chamados (paginado) |
| GET | `failures/open` | Lista chamados abertos |
| GET | `failures/closed` | Lista chamados fechados |
| GET | `failures/{id}` | Detalhe de um chamado específico |

### Works (Planos Mestres de Manutenção Preventiva)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `works` | Lista todos os planos mestres |
| GET | `works/{id}` | Detalhe de um plano mestre |

### Scheduled Works (Ocorrências / Execuções)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `works/scheduled` | Lista todas as ocorrências |
| GET | `works/scheduled/{id}` | Detalhe de uma ocorrência |

### Operators (Operadores/Técnicos)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `operators` | Lista todos os operadores |

---

## Filtros de Data por Endpoint (JQL validados)

Formato de data: `YYYY-MM-DDTHH:MM:SS`

### `failures`
| Filtro | Descrição |
|--------|-----------|
| `date_min_last_status_change_date` | Data mínima da última mudança de status |
| `date_max_last_status_change_date` | Data máxima da última mudança de status |

### `works`
| Filtro | Descrição |
|--------|-----------|
| `date_min_updated_at` | Data mínima de atualização |
| `date_max_updated_at` | Data máxima de atualização |

### `works/scheduled`
| Filtro | Descrição |
|--------|-----------|
| `date_min_updated_at` | Data mínima de atualização |
| `date_max_updated_at` | Data máxima de atualização |
| `date_min_start_date` | Data mínima de início agendado |
| `date_max_start_date` | Data máxima de início agendado |

> `date_min_last_status_change_date` NÃO é válido para `works/scheduled` (retorna 400).

---

## Parâmetro `expanded` — Expansões por Endpoint

O parâmetro `expanded` inclui objetos relacionados na resposta, evitando múltiplas chamadas.

### Failures — Bulk (lista)
```
expanded=events,operator,location
```

### Failures — Detalhe individual
```
expanded=operator,location,client,problem,events.registry
```
> `events.registry` inclui o log completo de ações (STARTED, PAUSED, RESUMED, COMPLETED) necessário para cálculo de horas.

### Works — Bulk e Detalhe
```
expanded=workPeriodicity,workSlaRules,workType,client,locations,operators
```

### Scheduled Works — Bulk (lista)
```
expanded=work.client,work.locations,work.operators,work.work_type
```

### Scheduled Works — Detalhe individual
```
expanded=work.client,work.locations,work.operators,work.work_type,audit_stats.category,events.registry
```

---

## Estrutura da Resposta

### Lista paginada
```json
{
  "data": [ { "id": "...", "type": "failure", "attributes": {...}, "relationships": {...} } ],
  "included": [...],
  "meta": { "pagination": { "total_pages": N, "total": N } }
}
```

### Detalhe individual
```json
{
  "data": { "id": "...", "type": "failure", "attributes": {...}, "relationships": {...} },
  "included": [
    { "id": "...", "type": "event", "attributes": { "operator_id": N, "status": "PAUSED" } },
    { "id": "...", "type": "event_registry", "attributes": { "event_id": N, "action": "STARTED", "date": "..." } },
    { "id": "...", "type": "location", "attributes": { "full_name": "CARMEL CHARME - ..." } },
    { "id": "...", "type": "operator", "attributes": { "full_name": "..." } }
  ]
}
```

---

## Códigos de Resposta Relevantes

| Código | Significado | Ação |
|--------|-------------|------|
| 200 | Sucesso | Processar normalmente |
| 400 | Parâmetro inválido (JQL) | Verificar filtros usados |
| 404 | Recurso não encontrado (foi deletado) | Marcar como EXCLUIDO no banco |
| 429 | Rate limit atingido | Aguardar `Retry-After` segundos |
| 5xx | Erro do servidor Infraspeak | Retry com backoff exponencial (2s, 4s) |

---

## Classe `ApiInfraspeak` — `etls/infraspeak/api.py`

```python
api_client = ApiInfraspeak(user, token)

# Requisição simples
response = api_client.request("failures/12345", {"expanded": "events.registry"})

# Todas as páginas de uma listagem
data = api_client.request_all_pages("failures", {
    "date_min_last_status_change_date": "2026-03-01T00:00:00",
    "expanded": "events,operator,location",
    "limit": 200
})
```

## Classe `RouteManager` — queries prontas

```python
from etls.infraspeak.api import RouteManager

endpoint, params = RouteManager.get_failures_bulk("2026-03-01", "2026-03-31")
endpoint, params = RouteManager.get_failures_delta("2026-03-14")
endpoint, params = RouteManager.get_open_failures()
endpoint, params = RouteManager.get_works_bulk()
```
