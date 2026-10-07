# wallet-service

Serviço em Go (Uber Fx) que processa operações financeiras de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) sobre carteiras de jogadores, por **HTTP** e por **fila SQS**, com várias réplicas idênticas. O PostgreSQL é a fonte da verdade e coordena as réplicas; Keycloak autentica; eventos de saída saem por *transactional outbox*.

As decisões de projeto (dinheiro, transações, idempotência, locks, referências pendentes, reversões, inbox/outbox, autenticação, Fx, desligamento, limitações) estão em [`.claude/ARCHITECTURE.md`](.claude/ARCHITECTURE.md). O enunciado está em [`.claude/backend-challenge-go.md`](.claude/backend-challenge-go.md).

## Sumário

1. [Pré-requisitos](#pré-requisitos)
2. [Subir o ambiente](#subir-o-ambiente)
3. [Variáveis de ambiente](#variáveis-de-ambiente)
4. [Filas SQS](#filas-sqs)
5. [Migrations: aplicar e reverter](#migrations-aplicar-e-reverter)
6. [Autenticação: identidades de teste](#autenticação-identidades-de-teste)
7. [Exemplos de chamadas](#exemplos-de-chamadas)
8. [Tracing (OpenTelemetry)](#tracing-opentelemetry)
9. [Testes](#testes)
10. [Estrutura do código](#estrutura-do-código)
11. [Problemas comuns](#problemas-comuns)

## Pré-requisitos

| Para | Precisa de |
|---|---|
| Subir o ambiente | Docker com Compose v2 (testado com 5.x) e `make` |
| Rodar os testes | Go 1.26.5 (a versão está no `go.mod` e no `Dockerfile`) e o ambiente acima de pé |
| Os exemplos deste README | `curl` e um shell POSIX (`sed` e `uuidgen` vêm no macOS e no Linux) |
| Migrations pelo `make migrate-*` no host (opcional) | [`goose`](https://github.com/pressly/goose) instalado. Pelo Compose não precisa de nada |

Portas usadas no host: `8080` (Keycloak), `8081`–`8083` (as três réplicas), `4566` (LocalStack) e `5432` (Postgres). Se a `5432` estiver ocupada, veja [Problemas comuns](#problemas-comuns).

## Subir o ambiente

```sh
make up          # o mesmo que: docker compose up --build -d
```

Isso constrói a imagem e sobe, nesta ordem: Postgres → `db-init` (cria os papéis `wallet_owner` e `wallet_app`) → `migrate` (aplica as migrations, uma vez) → as **3 réplicas** do serviço, e em paralelo Keycloak (já com o realm `wallet` importado) e LocalStack (já com as filas criadas). Do zero leva cerca de um minuto; o Keycloak é o mais lento.

Confira que as três réplicas estão prontas:

```sh
for p in 8081 8082 8083; do curl -s localhost:$p/health/ready; echo; done
# {"checks":{"postgres":"up","sqs":"up","sqs-inbound":"up"},"status":"UP"}
```

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

**Só do Compose** (todas opcionais): `POSTGRES_PASSWORD`, `POSTGRES_PORT`, `WALLET_OWNER_PASSWORD`, `WALLET_APP_PASSWORD`, `KEYCLOAK_ADMIN_PASSWORD`.

**Dos testes**: `TEST_DATABASE_URL`, `TEST_KEYCLOAK_URL`, `TEST_SQS_ENDPOINT` (o `Makefile` já traz valores locais).

## Filas SQS

Nada a fazer: o script [`deploy/localstack-init.sh`](deploy/localstack-init.sh) roda sozinho quando o LocalStack fica pronto (e de novo a cada reinício, é idempotente) e cria:

| Fila | Configuração |
|---|---|
| `wager-transactions.fifo` | Entrada. Visibilidade 30 s, long polling 20 s, redrive para a DLQ após **5** recebimentos |
| `wager-transactions-dlq.fifo` | DLQ, retenção de 14 dias |
| `wallet-events.fifo` | Saída: eventos da outbox |

Para conferir ou recriar manualmente:

```sh
docker compose exec localstack awslocal sqs list-queues
docker compose restart localstack     # roda o script de novo
```

## Migrations: aplicar e reverter

As migrations ficam em [`internal/infra/migrations`](internal/infra/migrations) (goose, com `Up` e `Down`). A aplicação **não** migra ao subir; quem aplica é o serviço `migrate` do Compose, uma vez, como `wallet_owner`.

```sh
make compose-migrate-status    # em que versão está
make compose-migrate-up        # aplica as pendentes
make compose-migrate-down      # reverte UMA migration (repita para voltar mais)
```

Fora do Compose, com `goose` instalado e o Postgres acessível em `DB_HOST`/`DB_PORT` (veja o `Makefile`): `make migrate-up`, `make migrate-down`, `make migrate-status`, `make migrate-reset`, `make migrate-create`.

## Autenticação: identidades de teste

O realm `wallet` é importado de [`deploy/keycloak-realm.json`](deploy/keycloak-realm.json) quando o Keycloak sobe. Todas as identidades usam `client_credentials`:

| `client_id` | `client_secret` | Papéis | Serve para |
|---|---|---|---|
| `wallet-internal` | `wallet-internal-secret` | `wallet:admin` | Serviço interno: abre carteiras, lê carteiras e extrato, reconcilia |
| `provider-a` | `provider-a-secret` | `wager:write`, `wager:read` | Provedor A: envia operações e lê **as suas** transações |
| `provider-b` | `provider-b-secret` | `wager:write`, `wager:read` | Provedor B, para testar o isolamento entre provedores |
| `provider-a-short` | `provider-a-short-secret` | os mesmos do A | Token que expira em 5 s, para testar token vencido |

O `providerId` vem do token (claim `provider_id`), nunca do corpo: um provedor que mande outro `providerId` no corpo recebe `403`. O console do Keycloak fica em <http://localhost:8080> (`admin` / `admin`).

Função auxiliar para pegar tokens (os exemplos abaixo a usam):

```sh
token() { curl -s -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" \
  http://localhost:8080/realms/wallet/protocol/openid-connect/token | sed -E 's/.*"access_token":"([^"]+)".*/\1/'; }
ADMIN=$(token wallet-internal wallet-internal-secret)
PROVIDER=$(token provider-a provider-a-secret)
```

Os tokens duram 5 minutos; se uma chamada devolver `401`, gere de novo.

## Exemplos de chamadas

Os valores monetários são sempre strings com duas casas (`"25.00"`). Respostas de erro seguem `application/problem+json` com um `code` estável.

**1. Abrir uma carteira** (só o serviço interno):

```sh
PLAYER=$(uuidgen | tr 'A-Z' 'a-z')
WALLET=$(curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}" | sed -E 's/.*"id":"([^"]+)".*/\1/')
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $ADMIN"
# {"id":"…","playerId":"…","balance":{"amount":"1000.00","currency":"BRL"},"version":1,…}
```

**2. Enviar uma aposta por HTTP** (o cabeçalho `Idempotency-Key` é obrigatório):

```sh
bet() { # $1 = id externo, $2 = tipo, $3 = valor, $4 = referência (opcional)
  ref=""; [ -n "$4" ] && ref=",\"referenceExternalTransactionId\":\"$4\""
  curl -s -w ' [%{http_code}]\n' -X POST localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H "Idempotency-Key: provider-a:$1" -H 'Content-Type: application/json' \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"$2\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}$ref}"
}
bet tx-1 BET 25.00        # 200 PROCESSED, balance 975.00, idempotentReplay false
bet tx-1 BET 25.00        # 200, idempotentReplay true: o resultado salvo, sem debitar de novo
bet tx-1 BET 99.00        # 409 IDEMPOTENCY_KEY_REUSED: mesma chave, conteúdo diferente
bet tx-2 BET 5000.00      # 422 REJECTED, failureCode INSUFFICIENT_FUNDS
```

| Situação | HTTP | `status` / `code` |
|---|---|---|
| Processada | `200` | `PROCESSED` |
| Repetição | `200` | o mesmo resultado, `idempotentReplay: true` |
| Recusada por regra de negócio | `422` | `REJECTED` + `failureCode` |
| Esperando a referência | `202` | `PENDING_REFERENCE` |
| Entrada inválida | `400` | `INVALID_REQUEST`, `INVALID_MONEY`, `VALIDATION_ERROR`, `MISSING_IDEMPOTENCY_KEY`, `KIND_NOT_ALLOWED` |
| Carteira inexistente ou de outro jogador | `422` | `WALLET_NOT_FOUND`, `PLAYER_WALLET_MISMATCH` |
| Chave ou ID externo reutilizados com outro conteúdo | `409` | `IDEMPOTENCY_KEY_REUSED`, `EXTERNAL_TRANSACTION_ID_CONFLICT` |
| Sem token / token inválido ou vencido | `401` | `UNAUTHENTICATED` |
| Sem o papel exigido | `403` | `FORBIDDEN` |
| Indisponibilidade temporária (banco) | `503` | `TEMPORARILY_UNAVAILABLE` |

A lista completa de códigos está no §8 do ARCHITECTURE.md.

**3. Um estorno que chega antes da aposta** (fica pendente e se resolve sozinho):

```sh
bet r-1 REFUND 10.00 tx-late    # 202 PENDING_REFERENCE
curl -s localhost:8081/providers/provider-a/wagering/transactions/r-1 -H "Authorization: Bearer $PROVIDER"
#   "status":"PENDING_REFERENCE", "attempts":N, "nextAttemptAt":…, "expiresAt":…
bet tx-late BET 10.00           # 200: a chegada da aposta acorda o estorno
sleep 2
curl -s localhost:8081/providers/provider-a/wagering/transactions/r-1 -H "Authorization: Bearer $PROVIDER"
#   "status":"PROCESSED"
```

Se a referência nunca chegar em `REFERENCE_TTL`, o estorno vira `REJECTED` com `REFERENCE_NOT_FOUND`.

**4. Consultar carteira, extrato e reconciliação** (só o serviço interno):

```sh
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $ADMIN"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $ADMIN"   # cursor opaco em "nextCursor"
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $ADMIN"
# {"walletId":…,"storedBalance":…,"calculatedBalance":…,"difference":{"amount":"0.00",…},"consistent":true,"checkedEntries":N}
```

**5. Enviar uma operação pela fila** (o grupo da mensagem é a carteira; o `SenderId` que o LocalStack informa é mapeado para o `provider-a`):

```sh
Q=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs send-message --queue-url "$Q" \
  --message-group-id "$WALLET" --message-deduplication-id "msg-1-$(date +%s)" --message-body "{
  \"messageId\":\"msg-1\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",
  \"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"sqs-1\",\"idempotencyKey\":\"provider-a:sqs-1\",
  \"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"BET\",
  \"money\":{\"amount\":\"5.00\",\"currency\":\"BRL\"}}}"
sleep 2
curl -s localhost:8081/providers/provider-a/wagering/transactions/sqs-1 -H "Authorization: Bearer $PROVIDER"   # PROCESSED
```

A mesma operação enviada por HTTP e por SQS é a mesma: repetir uma depois da outra devolve o resultado salvo. Mensagens que não têm como dar certo (JSON inválido, remetente que não é o provedor do corpo, `kind: OPENING`, mesma `messageId` com outro conteúdo…) vão direto para a DLQ, com o motivo no atributo `failureCode`:

```sh
docker compose exec -T localstack awslocal sqs send-message --queue-url "$Q" \
  --message-group-id "$WALLET" --message-deduplication-id "bad-$(date +%s)" --message-body '{"isto não é":'
D=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs receive-message --queue-url "$D" --message-attribute-names All --wait-time-seconds 3
#   "failureCode": "MALFORMED_MESSAGE"
```

**6. Eventos de saída** (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`), na fila `wallet-events.fifo`, com `MessageGroupId` = carteira e `MessageDeduplicationId` = `eventId`:

```sh
E=$(docker compose exec -T localstack awslocal sqs get-queue-url --queue-name wallet-events.fifo --query QueueUrl --output text | tr -d '\r')
docker compose exec -T localstack awslocal sqs receive-message --queue-url "$E" --max-number-of-messages 3 --attribute-names MessageGroupId --message-attribute-names All
```

O corpo é exatamente o JSON gravado na outbox na transação da operação.

**7. Saúde e métricas** (públicos):

```sh
curl -s localhost:8081/health/live
curl -s localhost:8081/health/ready
curl -s localhost:8081/metrics | grep -E '^(wager_|outbox_|pending_reference|sqs_|wallet_)' | grep -v _bucket
```

Cada réplica expõe só o que ela mesma tratou; some as três para o total. As métricas estão listadas no §11 do ARCHITECTURE.md. Os logs saem em JSON, com `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId`, e nunca com valores monetários em `INFO`.

## Tracing (OpenTelemetry)

Opcional e **desligado por padrão**: sem um endpoint OTLP o serviço não exporta nada. Para ver as trilhas:

```sh
make up-tracing        # o mesmo que `make up`, mais um Jaeger, com as 3 réplicas exportando para ele
```

Faça algumas chamadas (por exemplo, a aposta do exemplo 2) e abra <http://localhost:16686>, serviço `wallet-service`. Uma aposta por HTTP aparece como `POST /wagering/transactions` → `wager.execute` → `db.transaction`; por SQS, como `sqs.process` → `wager.execute` → `db.transaction`; a publicação dos eventos é `outbox.publish` e a retomada de uma pendência, `pending_reference.resolve`.

A trilha continua a do chamador: mande um cabeçalho `traceparent` no HTTP, ou o atributo de mensagem `traceparent` no SQS, e os spans entram nessa trilha (o `traceId` também aparece no log `wager transaction handled`).

```sh
curl -s -X POST localhost:8081/wagering/transactions -H "Authorization: Bearer $PROVIDER" -H "Idempotency-Key: provider-a:t-1" \
  -H 'Content-Type: application/json' -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' -d '{…}'
# Jaeger: http://localhost:16686/trace/4bf92f3577b34da6a3ce929d0e0e4736
```

Para usar outro coletor, defina `OTEL_EXPORTER_OTLP_ENDPOINT` (por exemplo `http://meu-coletor:4318`) para o serviço; `OTEL_TRACES_SAMPLER=parentbased_traceidratio` com `OTEL_TRACES_SAMPLER_ARG=0.1` amostra 10%. Os spans não levam valores monetários, tokens nem corpos de requisição. Voltar ao normal: `make up` (ou `make down` para parar tudo, Jaeger incluído).

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

A injeção de falhas (`pkg/faultinject`) só existe nos binários compilados com a tag `faultinject`; o binário normal não contém esse código (um teste confere).

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
deploy/                    realm do Keycloak, script de filas e políticas do LocalStack, papéis do banco
test/recovery/             a suíte de falhas com processos reais
```

## Problemas comuns

- **A porta 5432 já está em uso** (outro Postgres no host): `POSTGRES_PORT=5433 docker compose up --build -d` e `make test-integration TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable`.
- **`migrate` falha com `permission denied for table goose_db_version`**: o volume do Postgres vem de uma versão antiga, anterior aos papéis `wallet_owner`/`wallet_app`. Recomece com `docker compose down -v`.
- **Só um provedor consegue enviar por SQS no ambiente local**: o LocalStack informa o mesmo `SenderId` (`000000000000`) para qualquer remetente, e o Compose o mapeia para `provider-a`. Na AWS real cada provedor teria o próprio principal do IAM. Detalhes e a consequência para o isolamento estão no ARCHITECTURE.md (§7.4 e §14).
- **Uma mensagem enviada logo depois de parar um consumidor demora até 30 s**: o *long poll* abandonado ainda pode "receber" a mensagem no servidor, que fica invisível até o fim da visibilidade. É um atraso, não uma perda.
- **O Keycloak demora no primeiro boot**: as réplicas só sobem depois que ele fica saudável; `docker compose ps` mostra o estado.
