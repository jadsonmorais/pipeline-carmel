Infraspeak ETL Project
Este projeto automatiza a extração de dados da API Infraspeak v3 para monitoramento de manutenção. O sistema evoluiu de uma arquitetura baseada em arquivos JSON/Parquet (V1) para uma solução robusta de persistência em PostgreSQL utilizando campos JSONB (V2), visando máxima performance e integridade dos dados históricos.
🚀 Funcionalidades Principais
• Extração Automática: Sincronização diária de Chamados (Failures) e Trabalhos Agendados (Scheduled Works).
• Persistência Bruta (Objetivo 1): Armazenamento integral dos objetos JSON da API em tabelas PostgreSQL para evitar perda de dados e permitir reprocessamento futuro.
• Consultas Otimizadas (JQL): Uso de parâmetros como expanded para capturar relacionamentos (clientes, locais, operadores) em uma única chamada, e date_min_last_status_change_date para capturas incrementais.
• Gestão de Throttling: Tratamento automático do limite de 60 requisições por minuto através da leitura do header Retry-After.
• Transformação Analítica: Geração de visualizações (Views SQL) que calculam horas trabalhadas por operador e detalham checklists de auditoria.
🛠️ Requisitos e Tecnologias
• Linguagem: Python 3.x.
• Bibliotecas: requests (API), pandas (Transformação), psycopg2 (PostgreSQL), pyarrow (Parquet).
• Banco de Dados: PostgreSQL 12+ com suporte a JSONB.
• Autenticação: Personal Access Token (PAT) via protocolo Bearer SSL.
📁 Estrutura de Arquivos Críticos
• api_v2.py: Gerencia as requisições HTTP e a construção de rotas otimizadas com o RouteManager.
• utils_v2.py: Contém a lógica de conexão com o banco de dados e a função de Upsert (upsert_raw_data), que evita duplicidade de registros ao atualizar dados existentes.
• history_sync_v2.py: Script especializado para carga de dados históricos em janelas de tempo retroativas (conforme histórico de conversa).
• auto.bat: Script de lote para execução automatizada via Agendador de Tarefas do Windows.
⚙️ Configuração
1. Autenticação: As credenciais devem ser armazenadas em um arquivo .env (ou auth.json) contendo API_DATA_USER e API_DATA_TOKEN.
2. Ambiente Virtual: Recomenda-se o uso de um venv para isolamento das dependências.
3. Banco de Dados: Configure as variáveis de ambiente DB_HOST, DB_NAME, DB_USER e DB_PASS para a conexão com o PostgreSQL.
🔄 Fluxo de Dados (V2)
1. Extração Bulk: O script identifica registros alterados em uma janela de 3 dias para garantir a captura de atualizações retroativas.
2. Extração Detalhada: Para cada ID identificado, o sistema realiza uma chamada individual com a expansão events.registry para obter o log completo de atividades.
3. Persistência: O JSON completo é salvo no banco de dados.
4. Consumo: O Power BI conecta-se às Views SQL (ex: v_detalhe_chamados_failures), eliminando a necessidade de processamento pesado no Power Query.
⚠️ Tratamento de Erros e Limites
• Erro 429: O sistema aguarda o tempo especificado pela API antes de tentar novamente.
• Erros de Conexão (SSL): Implementação de blocos try/except com continue para garantir que instabilidades na rede não interrompam o processo de carga massiva.
• Monitoramento: Logs de entrega e erros de servidor (5xx) devem ser reportados ao suporte da Infraspeak conforme necessário.

--------------------------------------------------------------------------------
Este projeto é mantido como parte da infraestrutura de dados da Carmel Hotéis para gestão operacional de manutenção.