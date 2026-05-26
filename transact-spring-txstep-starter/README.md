# transact-spring-txstep-starter

Spring Boot auto-configuration for the `@TransactionalStep` annotation.

`@TransactionalStep` marks a Spring-managed method as an **idempotent transactional step** inside a DBOS workflow. Each call runs in a `REQUIRES_NEW` Spring transaction; the step's return value is written to a `tx_step_outputs` table **atomically** with the user's database work. On workflow retry, the recorded output is replayed without re-executing the method body.

## Installation

Add both the DBOS Spring Boot starter and this module:

**Gradle**
```kotlin
implementation("dev.dbos:transact-spring-boot-starter:<version>")
implementation("dev.dbos:transact-spring-txstep-starter:<version>")
```

**Maven**
```xml
<dependency>
  <groupId>dev.dbos</groupId>
  <artifactId>transact-spring-boot-starter</artifactId>
  <version>VERSION</version>
</dependency>
<dependency>
  <groupId>dev.dbos</groupId>
  <artifactId>transact-spring-txstep-starter</artifactId>
  <version>VERSION</version>
</dependency>
```

## Prerequisites

- Spring Boot 3.x or 4.x
- A PostgreSQL `DataSource` bean
- A `PlatformTransactionManager` bean (auto-configured by Spring Boot for all supported stacks)
- `dbos.application.name` property set

## Usage

Annotate any Spring-managed method with `@TransactionalStep`. The method must be called through a Spring proxy — inject the bean into a `@Workflow`-annotated method in another Spring bean.

### Spring JDBC / JdbcTemplate

No extra dependencies. Spring Boot auto-configures `DataSourceTransactionManager` and `JdbcTemplate`.

```java
@Service
public class OrderStepService {
    @Autowired JdbcTemplate jdbc;

    @TransactionalStep
    public Order saveOrder(Order order) {
        jdbc.update("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)",
            order.id(), order.item(), order.qty());
        return order;
    }
}

@Service
public class OrderWorkflowService {
    @Autowired OrderStepService steps;

    @Workflow
    public Order processOrder(Order order) {
        return steps.saveOrder(order);
    }
}
```

`JdbcTemplate` routes through `DataSourceUtils` internally, so it automatically joins the `REQUIRES_NEW` transaction started by the aspect. Spring Data JDBC repositories (`CrudRepository`) work identically.

### JDBI

Add `jdbi3-spring` so JDBI's `SpringTransactionHandler` reuses the active Spring transaction rather than opening a separate connection:

```kotlin
implementation("org.jdbi:jdbi3-spring:<version>")
```

```java
@Configuration
public class JdbiConfig {
    @Bean
    public Jdbi jdbi(DataSource dataSource) throws Exception {
        var factory = new JdbiFactoryBean(dataSource);
        factory.afterPropertiesSet();
        return factory.getObject();
    }
}

@Service
public class OrderStepService {
    @Autowired Jdbi jdbi;

    @TransactionalStep
    public Order saveOrder(Order order) {
        jdbi.withHandle(h ->
            h.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)",
                order.id(), order.item(), order.qty()));
        return order;
    }
}
```

### jOOQ

Spring Boot auto-configures `DSLContext` with `SpringTransactionProvider` when you add `spring-boot-starter-jooq`. Set `spring.jooq.sql-dialect=POSTGRES` and inject `DSLContext` directly:

```kotlin
implementation("org.springframework.boot:spring-boot-starter-jooq")
```

```java
@Service
public class OrderStepService {
    @Autowired DSLContext dsl;

    @TransactionalStep
    public Order saveOrder(Order order) {
        dsl.execute("INSERT INTO orders(id, item, qty) VALUES (?, ?, ?)",
            order.id(), order.item(), order.qty());
        return order;
    }
}
```

`spring-boot-starter-jooq` wraps the `DataSource` in a `TransactionAwareDataSourceProxy`, so jOOQ queries execute on the Spring-transaction-bound connection.

### JPA / Hibernate

Spring Boot auto-configures `JpaTransactionManager` when `spring-boot-starter-data-jpa` is present. `@TransactionalStep` detects `JpaTransactionManager` and sets its `dataSource` property automatically if not already configured, bridging JPA transactions to `DataSourceUtils`.

```kotlin
implementation("org.springframework.boot:spring-boot-starter-data-jpa")
```

```java
@Service
public class OrderStepService {
    @Autowired OrderRepository repo;  // Spring Data JPA repository

    @TransactionalStep
    public Order saveOrder(Order order) {
        return repo.save(order);
    }
}
```

Direct `EntityManager` access also works via `EntityManagerFactoryUtils.getTransactionalEntityManager(emf)`.

### MyBatis

Spring Boot auto-configures `SqlSessionTemplate` with `DataSourceTransactionManager` via `mybatis-spring-boot-starter`. MyBatis mappers participate in Spring transactions automatically:

```kotlin
implementation("org.mybatis.spring.boot:mybatis-spring-boot-starter:<version>")
```

```java
@Mapper
public interface OrderMapper {
    @Insert("INSERT INTO orders(id, item, qty) VALUES(#{id}, #{item}, #{qty})")
    void insert(@Param("id") String id, @Param("item") String item, @Param("qty") int qty);
}

@Service
public class OrderStepService {
    @Autowired OrderMapper orderMapper;

    @TransactionalStep
    public Order saveOrder(Order order) {
        orderMapper.insert(order.id(), order.item(), order.qty());
        return order;
    }
}
```

## Configuration

| Property | Default | Description |
|---|---|---|
| `dbos.txstep.schema` | DBOS system schema | PostgreSQL schema for the `tx_step_outputs` table |

The `tx_step_outputs` table is created lazily on startup — only if at least one `@TransactionalStep` method is found in the Spring context. Applications that never use the annotation incur no database contact.

## How it works

1. `TransactionalStepAspect` intercepts every `@TransactionalStep` call and delegates to `TransactionalStepFactory`.
2. `TransactionalStepFactory` calls `DBOS.runStep()`, which checks `tx_step_outputs` for a prior result. If one exists, it is returned immediately (idempotent replay).
3. Otherwise, a `REQUIRES_NEW` Spring transaction is started. The method body runs, and the result is written to `tx_step_outputs` using `DataSourceUtils.getConnection()` — the same connection the transaction holds.
4. The transaction commits, making the user's write and the step output record atomic. If the method throws, the transaction rolls back and the error is recorded separately so retries can replay it.
5. `TransactionalStepRegistrar` scans beans after context startup and calls `factory.initialize()` (which creates `tx_step_outputs`) only when annotated methods are found.
