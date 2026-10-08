Contexto: wallet-service, um serviço único em Go (Uber Fx) que processa operações financeiras de provedores de jogos sobre carteiras de jogadores. Entrada por HTTP e SQS FIFO, PostgreSQL como fonte da verdade, Keycloak (OIDC) para autenticação, eventos de saída por transactional outbox. Roda em várias réplicas idênticas e stateless; toda a coordenação acontece no banco. Fonte de verdade das decisões: `.claude/ARCHITECTURE.md`; enunciado em `.claude/backend-challenge-go.md`. Não há gRPC, Kafka nem microsserviços neste projeto.

Estilo de código:
- Comentários e identificadores em inglês. Comentários só quando o porquê não é óbvio pelo nome ou pela estrutura. Nunca descreva o que o código já deixa claro. Nunca escreva bloco de doc de múltiplos parágrafos, uma ou duas linhas bastam.
- Sem abstrações prematuras: três linhas parecidas são melhores que uma abstração genérica cedo demais. Só generalize quando o segundo uso real aparecer.
- Erros de negócio são sentinelas (errors.New), comparados com errors.Is, nunca com == depois de qualquer wrapping. Erros de infraestrutura ganham contexto com fmt.Errorf("...: %w", err) ao subir uma camada.
- Toda função que faz I/O recebe context.Context. panic nunca representa rejeição de negócio.
- Testes são table-driven por padrão. Um fake de teste implementa a interface mínima da porta, sem framework de mock.

Arquitetura (dependências apontam para dentro; detalhes em ARCHITECTURE.md §1.2)
- domain/ (money, wallet, wager, event) importa só a stdlib e outros pacotes de domain. Campos privados, construtores com validação, reidratação separada da criação.
- usecase/ (um pacote por caso de uso: openwallet, processwager, resolvereference, reconcile, queries) orquestra só contra as portas de usecase/port.go (UnitOfWork, repositórios, Clock, IDGenerator). É o caso de uso que delimita a transação SQL; repositórios nunca abrem transação própria.
- infra/ implementa as portas: postgres, sqs, oidc, outbox, refworker, migrations.
- adapter/ traduz transporte para comandos de caso de uso: httpapi (handlers, DTOs, problem+json, middlewares) e sqsmsg (envelope SQS). HTTP e SQS montam o mesmo comando e compartilham o caso de uso.
- app/ é o único lugar que conhece Fx (fx.Module, Provide, Invoke, Lifecycle). cmd/wallet-service/main.go só chama fx.New(...).Run().
- pkg/ são utilitários genéricos que não sabem o que é aposta (canonicaljson, backoff, faultinject).

Regras financeiras e de banco
- Dinheiro é Money (int64 em centavos, escala 2) em todo o código; nunca float32/float64, nem no parsing nem na serialização.
- Invariantes valem no banco (CHECK, UNIQUE, triggers), independentemente do código. Migrations goose em internal/infra/migrations, com Up e Down; a aplicação não migra no boot.
- Concorrência por carteira com SELECT ... FOR NO KEY UPDATE (FOR UPDATE conflita com o KEY SHARE das FKs e gera deadlock); carteira primeiro, depois as demais linhas. Nunca lock global nem mutex em memória como garantia entre réplicas.
- Evento só vai para a outbox, na mesma transação da operação; nada é publicado antes do commit.

Antes de considerar uma tarefa concluída
- Rode gofmt -l, go build ./..., go vet ./... e go test -race ./...
- Testes que precisam de Postgres, Keycloak ou LocalStack usam a build tag integration e o ambiente do docker-compose (make test-integration); não os substitua por mocks.
- Não adicione dependência fora da tabela de ARCHITECTURE.md §1.3 sem justificar por que a stdlib não resolve.
