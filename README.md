# wallet-service

Serviço em Go (Uber Fx) que processa operações financeiras de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) sobre carteiras de jogadores, por **HTTP** e por **fila SQS**, com várias réplicas idênticas. O PostgreSQL é a fonte da verdade e coordena as réplicas; Keycloak autentica; eventos de saída saem por *transactional outbox*.

As decisões de projeto (dinheiro, transações, idempotência, locks, referências pendentes, reversões, inbox/outbox, autenticação, Fx, desligamento, limitações) estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Sumário

1. [Pré-requisitos](#pré-requisitos)
2. [Subir o ambiente](#subir-o-ambiente)
3. [Variáveis de ambiente](#variáveis-de-ambiente)
4. [Migrations: aplicar e reverter](#migrations-aplicar-e-reverter)
5. [Rotas](#rotas)
6. [Autenticação: identidades de teste](#autenticação-identidades-de-teste)
7. [Exemplos de chamadas](#exemplos-de-chamadas)
8. [Tracing (OpenTelemetry)](#tracing-opentelemetry)
9. [Métricas e logs: Prometheus, Loki e Grafana](#métricas-e-logs-prometheus-loki-e-grafana)
10. [Testes](#testes)
11. [Estrutura do código](#estrutura-do-código)
12. [Problemas comuns](#problemas-comuns)

## Pré-requisitos

| Para | Precisa de |
|---|---|
| Subir o ambiente | Docker com Compose v2 (testado com 5.x) e `make` |
| Rodar os testes | Go 1.26.5 (a versão está no `go.mod` e no `Dockerfile`) e o ambiente acima de pé |
| Os exemplos deste README | Postman (ou outro cliente HTTP); os exemplos de fila usam um shell POSIX |
| Migrations pelo `make migrate-*` no host (opcional) | [`goose`](https://github.com/pressly/goose) instalado. Pelo Compose não precisa de nada |

Portas usadas no host: `8080` (Keycloak), `8081`–`8083` (as três réplicas), `4566` (LocalStack) e `5432` (Postgres). Se a `5432` estiver ocupada, veja [Problemas comuns](#problemas-comuns).

## Subir o ambiente

```sh
make up          # o mesmo que: docker compose up --build -d
```

Isso constrói a imagem e sobe, nesta ordem: Postgres → `db-init` (cria os papéis `wallet_owner` e `wallet_app`) → `migrate` (aplica as migrations, uma vez) → as **3 réplicas** do serviço, e em paralelo Keycloak (já com o realm `wallet` importado) e LocalStack (já com as filas criadas). Do zero leva cerca de um minuto; o Keycloak é o mais lento.

Para parar mantendo o banco: `make down`. Para recomeçar do zero, apagando também o volume do Postgres: `docker compose down -v`.

## Variáveis de ambiente

O Compose já define todas para o ambiente local. O arquivo [`.env.example`](.env.example) lista as do Compose e os valores locais de exemplo, sem segredos reais; copie para `.env` para sobrescrever.

**Serviço** (lidas em `internal/app/config.go`; uma configuração inválida impede o boot):

| Variável | Padrão | Para quê |
|---|---|---|
| `DATABASE_URL` | **obrigatória** | Postgres, como `wallet_app` (sem permissão de DDL) |
| `OIDC_ISSUER` | **obrigatória** | `iss` esperado nos tokens (`http://localhost:8080/realms/wallet`) |
| `OIDC_DISCOVERY_URL` | vazio | Onde buscar as chaves, se for diferente do emissor (dentro do Compose: `http://keycloak:8080/realms/wallet`) |
| `OIDC_AUDIENCE` | `wallet-api` | `aud` exigido nos tokens |
| `HTTP_ADDR` | `:8081` | Endereço do servidor HTTP |
| `REFERENCE_TTL` | `10m` | Quanto tempo uma operação espera a referência chegar antes de ser rejeitada |
| `REFERENCE_LEASE` | `30s` | Por quanto tempo uma réplica "segura" uma pendência que reservou |
| `OUTBOX_LEASE` | `30s` | Idem, para eventos da outbox |
| `AWS_REGION` | `us-east-1` | Região do SQS |
| `SQS_ENDPOINT` | vazio | Endpoint do LocalStack; vazio usa a AWS real |
| `EVENTS_QUEUE_NAME` / `EVENTS_QUEUE_URL` | `wallet-events.fifo` / vazio | Fila de saída (eventos). A URL, se informada, dispensa a busca pelo nome |
| `WAGER_QUEUE_NAME` / `WAGER_QUEUE_URL` | `wager-transactions.fifo` / vazio | Fila de entrada |
| `WAGER_DLQ_NAME` / `WAGER_DLQ_URL` | `wager-transactions-dlq.fifo` / vazio | DLQ da fila de entrada |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | vazio | Liga o tracing e diz para onde exportar (OTLP/HTTP). O resto vem das variáveis `OTEL_*` padrão (`OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`…). Veja [Tracing](#tracing-opentelemetry) |
| `SQS_SENDER_PROVIDER_MAP` | vazio | `senderId=providerId[,…]`: quem o SQS diz que enviou → qual provedor pode ser. Vazio: toda mensagem vai para a DLQ |

Credenciais da AWS vêm da cadeia padrão do SDK (no Compose: `AWS_ACCESS_KEY_ID=test`, aceito pelo LocalStack).

**Só do Compose** (todas opcionais): `POSTGRES_PASSWORD`, `POSTGRES_PORT`, `WALLET_OWNER_PASSWORD`, `WALLET_APP_PASSWORD`, `KEYCLOAK_ADMIN_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`.

**Dos testes**: `TEST_DATABASE_URL`, `TEST_KEYCLOAK_URL`, `TEST_SQS_ENDPOINT` (o `Makefile` já traz valores locais).

## Migrations: aplicar e reverter

As migrations ficam em [`internal/infra/migrations`](internal/infra/migrations) (goose, com `Up` e `Down`). A aplicação **não** migra ao subir; quem aplica é o serviço `migrate` do Compose, uma vez, como `wallet_owner`.

```sh
make compose-migrate-status    # em que versão está
make compose-migrate-up        # aplica as pendentes
make compose-migrate-down      # reverte UMA migration (repita para voltar mais)
```

Fora do Compose, com `goose` instalado e o Postgres acessível em `DB_HOST`/`DB_PORT` (veja o `Makefile`): `make migrate-up`, `make migrate-down`, `make migrate-status`, `make migrate-reset`, `make migrate-create`.

## Rotas

### Aplicação

Cada réplica atende em uma porta: `http://localhost:8081`, `:8082` e `:8083`. As três expõem as mesmas rotas e compartilham o mesmo banco.

| Método | Rota | Papel exigido | Para quê |
|---|---|---|---|
| `POST` | `/wallets` | `wallet:admin` | Abre uma carteira com saldo inicial (pode ser `"0.00"`) |
| `GET` | `/wallets/{walletId}` | `wallet:admin` | Saldo e versão da carteira |
| `GET` | `/wallets/{walletId}/ledger?cursor=…&limit=50` | `wallet:admin` | Extrato paginado (cursor opaco em `nextCursor`, `limit` de 1 a 200) |
| `POST` | `/wallets/{walletId}/reconciliation` | `wallet:admin` | Compara o saldo com o extrato, sem alterar nada |
| `POST` | `/wagering/transactions` | `wager:write` | Envia uma operação (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`); exige `Idempotency-Key` |
| `GET` | `/wagering/transactions/{transactionId}` | `wager:read` (só as próprias) ou `wallet:admin` | Consulta uma transação pelo ID interno |
| `GET` | `/providers/{providerId}/wagering/transactions/{externalTransactionId}` | `wager:read`, com `providerId` igual ao do token | Consulta uma transação pelo ID externo do provedor |
| `GET` | `/health/live` | Pública | O processo está respondendo |
| `GET` | `/health/ready` | Pública | Postgres e filas SQS acessíveis |
| `GET` | `/metrics` | Pública | Métricas no formato Prometheus |

Todas as rotas que exigem papel precisam do cabeçalho `Authorization: Bearer <token>`.

### Keycloak

Base: `http://localhost:8080`, realm `wallet`.

| Método | Rota | Para quê |
|---|---|---|
| `POST` | `/realms/wallet/protocol/openid-connect/token` | Emite o token (`grant_type=client_credentials`, `client_id`, `client_secret`) |
| `GET` | `/realms/wallet/.well-known/openid-configuration` | Documento de descoberta OIDC (emissor, endpoints, algoritmos) |
| `GET` | `/realms/wallet/protocol/openid-connect/certs` | Chaves públicas (JWKS) que o serviço usa para validar a assinatura |
| `GET` | `/admin` | Console de administração (`admin` / `admin`) |

### Exemplo de request

Uma aposta de ponta a ponta: obter o token do provedor e enviar a operação.

**1. Token** (`client_credentials` do `provider-a`):

```http
POST http://localhost:8080/realms/wallet/protocol/openid-connect/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id=provider-a&client_secret=provider-a-secret
```

```json
{"access_token":"eyJ...","expires_in":300,"token_type":"Bearer","scope":"profile email"}
```

**2. Operação** (a carteira precisa existir; veja o exemplo 1 de [Exemplos de chamadas](#exemplos-de-chamadas)):

```http
POST http://localhost:8081/wagering/transactions
Authorization: Bearer eyJ...
Idempotency-Key: provider-a:transaction-123
Content-Type: application/json

{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Correlation-Id: 01a11db2-ab17-765a-b636-49247517d36a

{"transactionId":"01a11db2-ab17-7b9f-9798-916c5fc3ba44","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

O `providerId` do corpo precisa ser o mesmo do token, senão a resposta é `403`. Para uma reversão (`REFUND` ou `ROLLBACK`), acrescente `"referenceExternalTransactionId"` ao corpo. Sem token, a resposta é:

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/problem+json
Www-Authenticate: Bearer realm="wallet"

{"type":"https://wallet.local/problems/unauthenticated","title":"Authentication required","status":401,"code":"UNAUTHENTICATED"}
```

## Autenticação: identidades de teste

O realm `wallet` é importado de [`deploy/keycloak-realm.json`](deploy/keycloak-realm.json) quando o Keycloak sobe. Todas as identidades usam `client_credentials`:

| `client_id` | `client_secret` | Papéis | Serve para |
|---|---|---|---|
| `wallet-internal` | `wallet-internal-secret` | `wallet:admin` | Serviço interno: abre carteiras, lê carteiras e extrato, reconcilia |
| `provider-a` | `provider-a-secret` | `wager:write`, `wager:read` | Provedor A: envia operações e lê **as suas** transações |
| `provider-b` | `provider-b-secret` | `wager:write`, `wager:read` | Provedor B, para testar o isolamento entre provedores |
| `provider-a-short` | `provider-a-short-secret` | os mesmos do A | Token que expira em 5 s, para testar token vencido |

O `providerId` vem do token (claim `provider_id`), nunca do corpo: um provedor que mande outro `providerId` no corpo recebe `403`. O console do Keycloak fica em <http://localhost:8080> (`admin` / `admin`).

Os tokens duram 5 minutos; se uma chamada devolver `401`, gere de novo.

## Exemplos de chamadas

Os exemplos abaixo são pensados para o **Postman** (ou Insomnia, Bruno etc.). Valores monetários são sempre strings com duas casas (`"25.00"`), e respostas de erro seguem `application/problem+json` com um campo `code` estável.

### Preparando o Postman

**1. Crie um environment** com estas variáveis:

| Variável | Valor inicial |
|---|---|
| `baseUrl` | `http://localhost:8081` (troque para `8082` ou `8083` para falar com outra réplica) |
| `tokenUrl` | `http://localhost:8080/realms/wallet/protocol/openid-connect/token` |
| `walletId` | vazio (preenchido pelo passo 1 abaixo) |
| `playerId` | vazio (preenchido pelo passo 1 abaixo) |

**2. Configure a autenticação.** São dois tokens diferentes, e usar o errado é a causa mais comum de `403`:

| Requests | Token de | Por quê |
|---|---|---|
| `/wallets/...` | `wallet-internal` | Só o serviço interno (`wallet:admin`) abre e lê carteiras |
| `/wagering/...` e `/providers/...` | `provider-a` | Só provedores (`wager:write`, `wager:read`) enviam operações |

O jeito mais simples é criar duas pastas na collection, **Interno** e **Provedor A**, e configurar a aba **Authorization** de cada pasta (os requests herdam com *Inherit auth from parent*):

| Campo | Pasta Interno | Pasta Provedor A |
|---|---|---|
| Type | OAuth 2.0 | OAuth 2.0 |
| Grant type | Client Credentials | Client Credentials |
| Access Token URL | `{{tokenUrl}}` | `{{tokenUrl}}` |
| Client ID | `wallet-internal` | `provider-a` |
| Client Secret | `wallet-internal-secret` | `provider-a-secret` |
| Client Authentication | Send client credentials in body | Send client credentials in body |

Clique em **Get New Access Token** → **Use Token**. O token vale 5 minutos; se um request devolver `401`, gere outro. As demais identidades de teste estão em [Autenticação](#autenticação-identidades-de-teste).

### 1. Abrir uma carteira (pasta Interno)

```http
POST {{baseUrl}}/wallets
Content-Type: application/json

{
  "playerId": "{{$guid}}",
  "initialBalance": { "amount": "1000.00", "currency": "BRL" }
}
```

Resposta `201`:

```json
{ "id": "01a11db2-aafa-719c-be24-fa72b00a2942", "playerId": "c98eb202-f3c4-4ca2-9e9f-dd734365213e",
  "balance": { "amount": "1000.00", "currency": "BRL" }, "version": 1 }
```

Para não copiar os IDs à mão, cole na aba **Scripts → Post-response** deste request:

```js
const body = pm.response.json();
pm.environment.set("walletId", body.id);
pm.environment.set("playerId", body.playerId);
```

Abrir de novo uma carteira para o mesmo `playerId` e moeda devolve `409 WALLET_ALREADY_EXISTS`. Saldo inicial `"0.00"` cria a carteira sem lançamento no extrato.

### 2. Enviar uma aposta (pasta Provedor A)

```http
POST {{baseUrl}}/wagering/transactions
Content-Type: application/json
Idempotency-Key: provider-a:tx-1

{
  "providerId": "provider-a",
  "externalTransactionId": "tx-1",
  "playerId": "{{playerId}}",
  "walletId": "{{walletId}}",
  "roundId": "round-1",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

O cabeçalho `Idempotency-Key` é obrigatório, e o `providerId` do corpo precisa ser o mesmo do token (senão, `403`). Variando este mesmo request dá para ver as garantias principais:

| O que mudar | Resposta |
|---|---|
| Nada (primeiro envio) | `200`, `status: PROCESSED`, `balance: 975.00`, `idempotentReplay: false` |
| Nada (reenviar igual) | `200`, o mesmo resultado com `idempotentReplay: true`; o saldo não muda |
| Só `money.amount` para `"99.00"` | `409 IDEMPOTENCY_KEY_REUSED`: mesma chave, conteúdo diferente |
| Chave para `provider-a:tx-1b`, mantendo `externalTransactionId: tx-1` | `409 EXTERNAL_TRANSACTION_ID_CONFLICT`: a mesma operação não pode entrar com outra chave |
| Novo ID (`tx-2` na chave e no corpo) e `money.amount` `"5000.00"` | `422`, `status: REJECTED`, `failureCode: INSUFFICIENT_FUNDS` |
| `providerId` para `"provider-b"` | `403`: o corpo não pode falar por outro provedor |
| `kind` para `"OPENING"` | `400 KIND_NOT_ALLOWED` |
| `money.amount` para `"25"` ou `25.00` (número) | `400 INVALID_MONEY` |
| Authorization para **No Auth** | `401 UNAUTHENTICATED` |

Os outros tipos usam o mesmo corpo, com `kind` diferente:

| `kind` | `money.amount` | Observação |
|---|---|---|
| `WIN` | maior que zero | Crédito; `referenceExternalTransactionId` é opcional |
| `LOSS` | exatamente `"0.00"` | Não mexe no saldo |
| `REFUND` | igual ao da aposta | Crédito; exige `"referenceExternalTransactionId": "<id externo da BET>"` |
| `ROLLBACK` | igual ao original | Desfaz uma `BET`, `WIN` ou `REFUND`; exige `referenceExternalTransactionId` |

Cada transação aceita uma única reversão com sucesso; a segunda devolve `422` com `REFERENCE_ALREADY_REVERSED`.

### 3. Um estorno que chega antes da aposta (pasta Provedor A)

Envie um `REFUND` que aponta para uma aposta que ainda não existe:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "r-1",
  "playerId": "{{playerId}}",
  "walletId": "{{walletId}}",
  "roundId": "round-1",
  "gameId": "fortune-chimp",
  "kind": "REFUND",
  "money": { "amount": "10.00", "currency": "BRL" },
  "referenceExternalTransactionId": "tx-late"
}
```

Com `Idempotency-Key: provider-a:r-1`, a resposta é `202` com `status: PENDING_REFERENCE`. Consulte o andamento:

```http
GET {{baseUrl}}/providers/provider-a/wagering/transactions/r-1
```

A resposta traz `attempts`, `nextAttemptAt` e `expiresAt`. Agora envie a `BET` `tx-late` de `10.00` (como no passo 2, com `Idempotency-Key: provider-a:tx-late`). Em um ou dois segundos, a consulta acima passa a mostrar `status: PROCESSED`. Se a aposta não chegar em `REFERENCE_TTL` (10 minutos), o estorno vira `REJECTED` com `REFERENCE_NOT_FOUND`.

### 4. Consultar transações (pasta Provedor A)

| Request | Para quê |
|---|---|
| `GET {{baseUrl}}/wagering/transactions/<transactionId>` | Pelo ID interno devolvido no envio |
| `GET {{baseUrl}}/providers/provider-a/wagering/transactions/tx-1` | Pelo ID externo |

Um provedor só enxerga as próprias transações. Com um token do `provider-b`, a primeira consulta devolve `404`, e a segunda (trocando o caminho para `provider-a`) devolve `403`.

### 5. Carteira, extrato e reconciliação (pasta Interno)

| Request | Resposta |
|---|---|
| `GET {{baseUrl}}/wallets/{{walletId}}` | Saldo atual e `version` |
| `GET {{baseUrl}}/wallets/{{walletId}}/ledger?limit=50` | Lançamentos em `items`; se houver mais, use o `nextCursor` em `?cursor=...` |
| `POST {{baseUrl}}/wallets/{{walletId}}/reconciliation` (sem corpo) | `storedBalance`, `calculatedBalance`, `difference`, `consistent: true` e `checkedEntries` |

### 6. Saúde e métricas (sem autenticação)

| Request | Resposta |
|---|---|
| `GET {{baseUrl}}/health/live` | `200` enquanto o processo responde |
| `GET {{baseUrl}}/health/ready` | `{"checks":{"postgres":"up","sqs":"up","sqs-inbound":"up"},"status":"UP"}` |
| `GET {{baseUrl}}/metrics` | Métricas no formato Prometheus |

Cada réplica expõe só o que ela mesma tratou. A lista de métricas está em [ARCHITECTURE.md](ARCHITECTURE.md#observabilidade).

### Códigos de resposta

| Situação | HTTP | `status` / `code` |
|---|---|---|
| Processada | `200` | `PROCESSED` |
| Repetição | `200` | o mesmo resultado, `idempotentReplay: true` |
| Recusada por regra de negócio | `422` | `REJECTED` + `failureCode` |
| Esperando a referência | `202` | `PENDING_REFERENCE` |
| Entrada inválida | `400` | `INVALID_REQUEST`, `INVALID_MONEY`, `VALIDATION_ERROR`, `MISSING_IDEMPOTENCY_KEY`, `KIND_NOT_ALLOWED` |
| Carteira inexistente ou de outro jogador | `422` | `WALLET_NOT_FOUND`, `PLAYER_WALLET_MISMATCH` |
| Chave ou ID externo reutilizados com outro conteúdo | `409` | `IDEMPOTENCY_KEY_REUSED`, `EXTERNAL_TRANSACTION_ID_CONFLICT` |
| Sem token, token inválido ou vencido | `401` | `UNAUTHENTICATED` |
| Sem o papel exigido | `403` | `FORBIDDEN` |
| Indisponibilidade temporária (banco) | `503` | `TEMPORARILY_UNAVAILABLE` |

A lista completa de `failureCode` está em [ARCHITECTURE.md](ARCHITECTURE.md#códigos-de-falha).

### Pela fila SQS (terminal)

O Postman não fala com o SQS, então estes exemplos usam o `awslocal` de dentro do contêiner do LocalStack. Defina `WALLET` e `PLAYER` com os valores do passo 1:

```sh
WALLET=<walletId>; PLAYER=<playerId>
Q=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs send-message --queue-url "$Q" \
  --message-group-id "$WALLET" --message-deduplication-id "msg-1-$(date +%s)" --message-body "{
  \"messageId\":\"msg-1\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",
  \"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"sqs-1\",\"idempotencyKey\":\"provider-a:sqs-1\",
  \"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
  \"money\":{\"amount\":\"5.00\",\"currency\":\"BRL\"}}}"
```

Depois, no Postman, `GET {{baseUrl}}/providers/provider-a/wagering/transactions/sqs-1` mostra `PROCESSED`. O `MessageGroupId` é a carteira, e o `SenderId` informado pelo LocalStack é mapeado para o `provider-a`. Enviar a mesma operação por HTTP e por SQS gera um único débito: a segunda chegada devolve o resultado salvo.

Mensagens que não têm como dar certo (JSON inválido, remetente que não é o provedor do corpo, `kind: OPENING`, mesmo `messageId` com outro conteúdo) vão direto para a DLQ, com o motivo no atributo `failureCode`:

```sh
docker compose exec -T localstack awslocal sqs send-message --queue-url "$Q" \
  --message-group-id "$WALLET" --message-deduplication-id "bad-$(date +%s)" --message-body '{"isto não é":'
D=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs receive-message --queue-url "$D" --message-attribute-names All --wait-time-seconds 3
#   "failureCode": "MALFORMED_MESSAGE"
```

Os eventos de saída (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`) ficam na fila `wallet-events.fifo`, com `MessageGroupId` igual à carteira e `MessageDeduplicationId` igual ao `eventId`:

```sh
E=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wallet-events.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs receive-message --queue-url "$E" --max-number-of-messages 3 --attribute-names MessageGroupId --message-attribute-names All
```

O corpo de cada mensagem é exatamente o JSON gravado na outbox, na mesma transação da operação.

## Tracing (OpenTelemetry)

Opcional e **desligado por padrão**: sem um endpoint OTLP o serviço não exporta nada. Para ver as trilhas:

```sh
make up-tracing        # o mesmo que `make up`, mais um Jaeger, com as 3 réplicas exportando para ele
```

Faça algumas chamadas (por exemplo, a aposta do exemplo 2) e abra <http://localhost:16686>, serviço `wallet-service`. Uma aposta por HTTP aparece como `POST /wagering/transactions` → `wager.execute` → `db.transaction`; por SQS, como `sqs.process` → `wager.execute` → `db.transaction`; a publicação dos eventos é `outbox.publish` e a retomada de uma pendência, `pending_reference.resolve`.


## Métricas e logs: Prometheus, Loki e Grafana

Opcional, para acompanhar o serviço rodando. Sobe junto com o Jaeger (os logs com `traceId` linkam para a trilha):

```sh
make up-observability      # o mesmo que `make up`, mais Jaeger, Prometheus, Loki, Alloy e Grafana
```

| O quê | Onde | Para quê |
|---|---|---|
| **Grafana** | <http://localhost:3000> (`admin` / `admin`; sem login só dá para ver) | Dashboard e consulta de logs |
| Prometheus | <http://localhost:9090> | `/targets` (as 3 réplicas), `/alerts` (as regras), consultas PromQL |
| Loki | <http://localhost:3100> | Armazena os logs (use pelo Grafana) |
| Alloy | <http://localhost:12345> | Coletor que lê os logs dos contêineres e os manda ao Loki |
| Jaeger | <http://localhost:16686> | Trilhas |

**Dashboard.** No Grafana: *Dashboards* → pasta **Wallet service** → **Wallet service**. Já vem carregado (nada a importar) e se atualiza a cada 10 s. Tem as linhas:
- **Visão geral:** operações por segundo, réplicas no ar, carteiras divergentes, referências pendentes, atraso da outbox, mensagens na DLQ.
- **Operações:** por resultado, rejeições por código, erros por classe, por canal e tipo, replays e conflitos de idempotência.
- **Latência e banco:** p50/p95/p99 do processamento, espera pelo lock da carteira, retentativas do banco por SQLSTATE, goroutines e memória por réplica.
- **Mensageria:** mensagens de entrada por desfecho, publicação da outbox, backlog da outbox, pendências de referência.
- **Logs:** avisos e erros de todas as réplicas, uma busca livre (a caixa **Search logs** no alto: cole um `correlationId`, `transactionId`, `walletId` ou `messageId`) e as linhas por nível.

Uma linha com `traceId` mostra o botão **Open trace**, que abre a trilha no Jaeger (o `X-Correlation-Id` da resposta HTTP é o `correlationId`).

**Alertas.** O Prometheus avalia 7 regras (`deploy/observability/prometheus/alerts.yml`): saldo divergente do ledger, réplica fora do ar, outbox atrasada, mensagens na DLQ, fila de entrada ilegível, banco pedindo retentativas e falhas do próprio serviço. Elas aparecem em <http://localhost:9090/alerts>; **não há Alertmanager**, então nada é notificado.

Os dados ficam em volumes do Docker (sobrevivem a `make down`); `docker compose down -v` apaga tudo, banco incluído. É um ambiente de desenvolvimento: o Grafana aceita visitantes sem login e o Alloy monta o socket do Docker (somente leitura).

## Testes

```sh
go build ./...
go vet ./...
go test ./...                 # unitários
go test -race ./...           # unitários com detector de corrida
```

Os testes de **integração** e de **recuperação** usam o Postgres, o Keycloak e o LocalStack **reais** do Compose, então o ambiente precisa estar de pé (`make up`). Eles criam bancos e filas descartáveis; não mexem nos dados do ambiente.

| O que | Comando | Observação |
|---|---|---|
| Integração (schema, repositórios, outbox, consumidor, Fx de ponta a ponta, autenticação, isolamento entre provedores, reconciliação, métricas) | `make test-integration` | Build tag `integration`, com `-race`. Cerca de 1 a 2 minutos |
| Falhas com 3 processos (morte depois do commit e antes do delete, antes do commit, depois de publicar, depois de reservar uma pendência; 50 repetições; 80 + 80; reinício) | `make test-recovery` | Tags `integration faultinject`. Compila o serviço com injeção de falhas e roda cada réplica como processo do SO. Cerca de 1 a 2 minutos |

Os valores padrão do `Makefile` supõem o Postgres na `localhost:5432` com a senha `postgres`; para outro endereço: `make test-integration TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable`. Sem as variáveis `TEST_*`, os testes de integração **se pulam** em vez de falhar.

## Estrutura do código

```
cmd/wallet-service/        main: só fx.New(...).Run()
cmd/migrate/               aplica/reverte as migrations (usado pelo Compose)
internal/domain/           regras puras: money, wallet, wager, event (só biblioteca padrão)
internal/usecase/          casos de uso: openwallet, processwager, resolvereference, reconcile, queries
internal/adapter/          httpapi (HTTP) e sqsmsg (envelope SQS → o mesmo comando do HTTP)
internal/infra/           postgres, sqs, oidc, outbox, refworker, observability, telemetry, migrations
internal/app/              módulos Fx: o único lugar que conhece o Fx
pkg/                       utilitários genéricos: canonicaljson, backoff, uuid, faultinject
deploy/                    realm do Keycloak, script de filas e políticas do LocalStack, papéis do banco, observability/ (Prometheus, Loki, Alloy, Grafana)
test/recovery/             a suíte de falhas com processos reais
```
