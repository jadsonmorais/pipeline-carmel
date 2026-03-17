# Skill: NF-e ETL (XMLs via SMB)

## Objetivo

Ler os arquivos XML de NF-e/NFC-e das pastas compartilhadas (SMB) e persistir no banco para rastrear quais notas foram enviadas ao fiscal.

---

## Estrutura do Módulo

```
etls/nfe/
  __init__.py       — vazio
  smb_client.py     — cliente SMB (lista e lê arquivos das pastas compartilhadas)
  parser.py         — parseia XML NF-e, extrai campos para JSONB
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

Não há filtro por data — o ETL varre tudo que está nas pastas. O upsert garante idempotência.

---

## Fluxo

1. `smb_client.SMBShareClient.__enter__` → abre conexão SMB com `10.197.0.51:445`
2. Para cada hotel em `HOTEL_SHARES`, lista arquivos `.xml` do share correspondente
3. Lê o conteúdo de cada arquivo
4. `parser.parse_xml(xml_content, hotel, filename)` → extrai campos e retorna dict
5. `utils.upsert_raw_data('nfe_raw_xmls', 'nota_id', records, 'nota')`

---

## Convenção de Nome de Arquivo

```
NFe{44_digitos_chave}-nfe.xml
Exemplo: NFe23260319253187000163650010000170031192531872-nfe.xml
```

A chave de 44 dígitos é extraída do atributo `Id` do elemento `infNFe` no XML (sem o prefixo "NFe").

---

## Tratamento de Erros

- Erros de leitura de arquivo individual → log + continua próximo arquivo
- Erros de parse XML → log + continua (registro não é persistido)
- Erro de conexão ao share → log + continua próximo hotel
- Arquivos sem chave válida de 44 dígitos → `ValueError` + skip

---

## Dependência

`smbprotocol==1.15.0` (em `requirements.txt`)
