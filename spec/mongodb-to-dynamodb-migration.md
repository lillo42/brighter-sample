# MongoDB to DynamoDB Migration Plan

## Overview

This document outlines the migration strategy from MongoDB to DynamoDB for the Brighter messaging infrastructure components (Inbox, Outbox, and Distributed Locking).

## Current State

### MongoDB Dependencies (BrighterSimple.csproj)

```xml
<PackageReference Include="Paramore.Brighter.Inbox.MongoDb" Version="10.0.2" />
<PackageReference Include="Paramore.Brighter.Locking.MongoDb" Version="10.0.2" />
<PackageReference Include="Paramore.Brighter.Outbox.MongoDb" Version="10.0.2" />
```

### Current Configuration (Program.cs)

```csharp
const string connectionString = "mongodb://root:example@localhost:27017";
var configuration = new MongoDbConfiguration(connectionString, "brighter")
{
    Inbox = new MongoDbCollectionConfiguration { Name = "inbox" },
    Outbox = new MongoDbCollectionConfiguration { Name = "outbox" },
    Locking = new MongoDbCollectionConfiguration { Name = "locking" },
};
```

### MongoDB Components in Use

| Component | Type | Purpose |
|-----------|------|---------|
| `MongoDbInbox` | Inbox | Duplicate message detection |
| `MongoDbOutbox` | Outbox | Message persistence for "at least once" delivery |
| `MongoDbLockingProvider` | Distributed Lock | Prevent duplicate processing |
| `MongoDbConnectionProvider` | Connection Provider | MongoDB connection management |
| `MongoDbUnitOfWork` | Transaction Provider | Atomic operations |

---

## Target State

### DynamoDB Dependencies

```xml
<PackageReference Include="Paramore.Brighter.Inbox.DynamoDb" Version="10.3.0" />
<PackageReference Include="Paramore.Brighter.Locking.DynamoDb" Version="10.3.0" />
<PackageReference Include="Paramore.Brighter.Outbox.DynamoDb" Version="10.3.0" />
<PackageReference Include="AWSSDK.DynamoDBv2" Version="4.0.15" />
```

### DynamoDB Configuration

```csharp
var dynamoDbConfig = new DynamoDbConfiguration(
    tableNamePrefix: "brighter-",
    region: RegionEndpoint.USEast1,  // or from config
    serviceUrl: "http://localhost:8000"  // for local DynamoDB
);
```

---

## Migration Phases

### Phase 1: Infrastructure Setup

#### 1.1 Update docker-compose.yml

Add DynamoDB Local container:

```yaml
services:
  dynamodb-local:
    image: amazon/dynamodb-local:latest
    ports:
      - "8000:8000"
    command: "-jar DynamoDBLocal.jar -sharedDb"
```

Remove or keep MongoDB for rollback capability during migration.

#### 1.2 Create DynamoDB Tables

Required tables for Brighter DynamoDB implementation:

| Table Name | Purpose | Key Schema |
|------------|---------|------------|
| `brighter-inbox` | Duplicate detection | Partition: `MessageId` |
| `brighter-outbox` | Message storage | Partition: `Topic`, Sort: `MessageId` |
| `brighter-locking` | Distributed locks | Partition: `ResourceName` |

### Phase 2: Package Migration

#### 2.1 Remove MongoDB Packages

```bash
dotnet remove package Paramore.Brighter.Inbox.MongoDb
dotnet remove package Paramore.Brighter.Locking.MongoDb
dotnet remove package Paramore.Brighter.Outbox.MongoDb
```

#### 2.2 Add DynamoDB Packages

```bash
dotnet add package Paramore.Brighter.Inbox.DynamoDb --version 10.3.0
dotnet add package Paramore.Brighter.Locking.DynamoDb --version 10.3.0
dotnet add package Paramore.Brighter.Outbox.DynamoDb --version 10.3.0
dotnet add package AWSSDK.DynamoDBv2
```

### Phase 3: Code Changes

#### 3.1 Update Usings (Program.cs)

**Remove:**
```csharp
using Paramore.Brighter.Inbox.MongoDb;
using Paramore.Brighter.Locking.MongoDb;
using Paramore.Brighter.MongoDb;
using Paramore.Brighter.Outbox.MongoDb;
```

**Add:**
```csharp
using Paramore.Brighter.Inbox.DynamoDb;
using Paramore.Brighter.Locking.DynamoDb;
using Paramore.Brighter.Outbox.DynamoDb;
using Amazon.DynamoDBv2;
using Amazon;
```

#### 3.2 Update Configuration Section

**Replace (lines 40-51):**
```csharp
const string connectionString = "mongodb://root:example@localhost:27017";

var configuration = new MongoDbConfiguration(connectionString, "brighter")
{
    Inbox = new MongoDbCollectionConfiguration { Name = "inbox" },
    Outbox = new MongoDbCollectionConfiguration { Name = "outbox" },
    Locking = new MongoDbCollectionConfiguration { Name = "locking" },
};

services.AddSingleton<IAmAMongoDbConfiguration>(configuration);
```

**With:**
```csharp
// DynamoDB Local for development
var dynamoDbClient = new AmazonDynamoDBClient(
    new AmazonDynamoDBConfig
    {
        ServiceURL = "http://localhost:8000",
        AuthenticationRegion = "us-east-1"
    }
);

var dynamoDbConfig = new DynamoDbConfiguration(
    dynamoDbClient,
    tableNamePrefix: "brighter-"
);

services.AddSingleton<IAmazonDynamoDB>(dynamoDbClient);
```

#### 3.3 Update Consumer Configuration (lines 76-77)

**Replace:**
```csharp
opt.InboxConfiguration = new InboxConfiguration(new MongoDbInbox(configuration));
```

**With:**
```csharp
opt.InboxConfiguration = new InboxConfiguration(new DynamoDbInbox(dynamoDbConfig));
```

#### 3.4 Update Producer Configuration (lines 79-102)

**Replace:**
```csharp
.AddProducers(opt =>
{
    opt.Outbox = new MongoDbOutbox(configuration);
    opt.DistributedLock = new MongoDbLockingProvider(configuration);
    opt.ConnectionProvider = typeof(MongoDbConnectionProvider);
    opt.TransactionProvider = typeof(MongoDbUnitOfWork);
    // ... rest of configuration
})
```

**With:**
```csharp
.AddProducers(opt =>
{
    opt.Outbox = new DynamoDbOutbox(dynamoDbConfig);
    opt.DistributedLock = new DynamoDbLockingProvider(dynamoDbConfig);
    opt.ConnectionProvider = typeof(DynamoDbConnectionProvider);
    opt.TransactionProvider = typeof(DynamoDbUnitOfWork);
    // ... rest of configuration
})
```

### Phase 4: Table Initialization

#### 4.1 Create Table Initialization Script

Create `scripts/init-dynamodb.sh`:

```bash
#!/bin/bash
# Initialize DynamoDB tables for Brighter

aws dynamodb create-table \
    --table-name brighter-inbox \
    --attribute-definitions AttributeName=MessageId,AttributeType=S \
    --key-schema AttributeName=MessageId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url http://localhost:8000

aws dynamodb create-table \
    --table-name brighter-outbox \
    --attribute-definitions AttributeName=Topic,AttributeType=S AttributeName=MessageId,AttributeType=S \
    --key-schema AttributeName=Topic,KeyType=HASH AttributeName=MessageId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url http://localhost:8000

aws dynamodb create-table \
    --table-name brighter-locking \
    --attribute-definitions AttributeName=ResourceName,AttributeType=S \
    --key-schema AttributeName=ResourceName,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url http://localhost:8000
```

### Phase 5: Testing & Validation

#### 5.1 Test Scenarios

| Scenario | Expected Result |
|----------|-----------------|
| Create new order | Message stored in DynamoDB outbox |
| Duplicate message | Detected via DynamoDB inbox |
| Concurrent processing | Distributed lock prevents duplicates |
| Outbox sweeper | Dispatches messages from DynamoDB |

#### 5.2 Validation Commands

```bash
# Check inbox items
aws dynamodb scan --table-name brighter-inbox --endpoint-url http://localhost:8000

# Check outbox items
aws dynamodb scan --table-name brighter-outbox --endpoint-url http://localhost:8000

# Check locks
aws dynamodb scan --table-name brighter-locking --endpoint-url http://localhost:8000
```

---

## Implementation Checklist

- [ ] Add DynamoDB Local to docker-compose.yml
- [ ] Remove MongoDB NuGet packages
- [ ] Add DynamoDB NuGet packages
- [ ] Update Program.cs using statements
- [ ] Replace MongoDB configuration with DynamoDB
- [ ] Replace MongoDbInbox with DynamoDbInbox
- [ ] Replace MongoDbOutbox with DynamoDbOutbox
- [ ] Replace MongoDbLockingProvider with DynamoDbLockingProvider
- [ ] Replace connection provider
- [ ] Replace transaction provider
- [ ] Create table initialization scripts
- [ ] Test message creation flow
- [ ] Test duplicate detection
- [ ] Test distributed locking
- [ ] Update documentation (README.md)

---

## Rollback Plan

If issues arise during migration:

1. **Code Rollback**: Revert to previous git commit
2. **Data Recovery**: MongoDB data remains intact until explicitly deleted
3. **Switch Back**: Change configuration to point back to MongoDB

---

## Production Considerations

### AWS DynamoDB (vs Local)

For production AWS deployment:

```csharp
// Production configuration
var dynamoDbClient = new AmazonDynamoDBClient(
    new BasicAWSCredentials(accessKey, secretKey),
    RegionEndpoint.USEast1  // or appropriate region
);
```

### Capacity Planning

| Table | Read Capacity | Write Capacity | Notes |
|-------|--------------|----------------|-------|
| inbox | Low | Low | Check on receive |
| outbox | Medium | High | Write on publish, read on sweep |
| locking | Low | Medium | Short TTL on locks |

Consider using DynamoDB On-Demand for variable workloads.

---

## References

- [Brighter DynamoDB Documentation](https://paramore.readthedocs.io/)
- [DynamoDB Local Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
- [AWS DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
