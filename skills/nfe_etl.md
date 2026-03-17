# Skill: NF-e ETL (XMLs via SMB)

## Objetivo

Ler os arquivos XML de NF-e/NFC-e (notas e cancelamentos) das pastas compartilhadas (SMB) e persistir no banco para rastrear quais notas foram enviadas ao fiscal e quais foram canceladas.

---

## Estrutura do Módulo

```
etls/nfe/
  __init__.py       — vazio
  smb_client.py     — cliente SMB (lista e lê arquivos das pastas compartilhadas)
  parser.py         — parseia XML NF-e e cancelamentos, extrai campos para JSONB
  sync.py           — orquestrador (entry point)
```

---

## Shares SMB

| Hotel    | Share                          |
|----------|-------------------------------|
| CUMBUCO  | `\\10.197.0.51\Cumbuco`       |
| CHARME   | `\\10.197.0.51\Charme`        |
| MAGNA    | `\\10.197.0.51\Magna`         |
| TAIBA    | `\\10.197.0.51\Taiba`         |

---

## Variáveis de Ambiente

```
NFE_SMB_HOST=10.197.0.51
NFE_SMB_USER=<usuario>
NFE_SMB_PASS=<senha>
NFE_SMB_DOMAIN=        # opcional, deixar vazio se workgroup
```

---

## Execução

```bash
# Sincroniza todos os hotéis (varredura completa dos 4 shares)
python -m etls.nfe.sync
```

Não há filtro por data — o ETL varre tudo que está nas pastas. O upsert garante idempotência. A cada execução, o sync carrega os IDs já existentes no banco e skipa arquivos já processados (eficiente para runs incrementais).

---

## Convenção de Nome de Arquivo

```
NFe{44_digitos_chave}-nfe.xml        # NF-e / NFC-e (nota fiscal)
NFe{44_digitos_chave}-can.xml        # Cancelamento de NF-e
```

Exemplos:
```
NFe23260319253187000163650010000170031192531872-nfe.xml
NFe23260319253187000163650010000170031192531872-can.xml
```

---

## Tabelas Destino

| Arquivo     | Tabela                          | PK               |
|-------------|----------------------------------|------------------|
| `*-nfe.xml` | `carmel.nfe_raw_xmls`           | `nota_id` (44 dígitos do `infNFe/@Id`) |
| `*-can.xml` | `carmel.nfe_raw_cancelamentos`  | `cancelamento_id` (`infEvento/@Id`) |

---

## Fluxo

1. `smb_client.SMBShareClient.__enter__` → abre conexão SMB com `10.197.0.51:445`
2. `utils.get_existing_nfe_ids()` / `utils.get_existing_cancelamento_ids()` → carrega IDs já no banco
3. Para cada hotel em `HOTEL_SHARES`:
   - `client.iter_xml_files(skip_ids=...)` → lista e lê `*-nfe.xml`, pulando IDs conhecidos
   - `parser.parse_xml(xml_content, hotel, filename)` → extrai campos do `infNFe`
   - Flush a cada 500 registros → `utils.upsert_raw_data('nfe_raw_xmls', 'nota_id', ...)`
   - `client.iter_cancelamento_files(skip_ids=...)` → lista e lê `*-can.xml`
   - `parser.parse_cancelamento(xml_content, hotel, filename)` → extrai campos do `infEvento`
   - Flush a cada 500 → `utils.upsert_raw_data('nfe_raw_cancelamentos', 'cancelamento_id', ...)`

---

## Parser: campos extraídos

### `parse_xml` (→ `nfe_raw_xmls`)

Suporta `<NFe>` avulso e `<nfeProc>` (envelope com `<NFe>` + `<protNFe>` dentro).

Campos principais: `nota_id`, `hotel`, `dhEmi`, `nNF`, `serie`, `mod`, `tpAmb`, `cnpj_emit`, `nome_emit`, `vNF`, `nProt`, `cStat`, `dhRecbto`, `xml_content`.

### `parse_cancelamento` (→ `nfe_raw_cancelamentos`)

Campos principais: `cancelamento_id`, `chNFe` (FK para `nfe_raw_xmls.nota_id`), `hotel`, `dhEvento`, `tpEvento` (110111), `nSeqEvento`, `cnpj`, `nProt`, `xJust`, `xml_content`.

---

## Tratamento de Erros

- Erros de leitura de arquivo individual → log + continua próximo arquivo
- Erros de parse XML → log + continua (registro não é persistido)
- Erro de conexão ao share → log + continua próximo hotel
- Arquivos sem chave válida de 44 dígitos → `ValueError` + skip

---

## Dependência

`smbprotocol==1.15.0` (em `requirements.txt`)
