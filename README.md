# Brighter Sample - DynamoDB Migration

This sample demonstrates the Paramore Brighter framework using **DynamoDB** for inbox, outbox, and distributed locking with **AWS SDK v4**.

## Prerequisites

- Docker and Docker Compose
- .NET 10.0 SDK
- AWS CLI (for table initialization)

## Running the Sample

### 1. Start Infrastructure Services

```bash
docker-compose up -d
```

This starts:
- **DynamoDB Local** on port 8000
- **MongoDB** on port 27017 (kept for rollback)
- **Kafka** on port 9092
- **Schema Registry** on port 8081
- **Control Center** on port 9021

### 2. Initialize DynamoDB Tables

```bash
./scripts/init-dynamodb.sh
```

This creates the required tables:
- `brighter-inbox` - Duplicate message detection
- `brighter-outbox` - Message persistence
- `brighter-locking` - Distributed locks

### 3. Run the Application

```bash
dotnet run
```

### 4. Test the Application

Type order values when prompted. The application will:
1. Create a new order
2. Deposit `OrderPlaced` event to outbox
3. Deposit `OrderPaid` event to outbox (if value is not divisible by 3)

Example:
```
Type an order value (or q to quit): 100
Type an order value (or q to quit): 50
Type an order value (or q to quit): q
```

## Architecture

### Components

| Component | Implementation | Purpose |
|-----------|---------------|---------|
| Inbox | `DynamoDbInboxV4` | Duplicate message detection |
| Outbox | `DynamoDbOutboxV4` | Message persistence for "at least once" delivery |
| Distributed Lock | `DynamoDbLockingProviderV4` | Prevent duplicate processing |
| Connection Provider | `DynamoDbConnectionProviderV4` | DynamoDB connection management |
| Transaction Provider | `DynamoDbUnitOfWorkV4` | Atomic operations |

### AWS SDK v4

This sample uses AWS SDK v4 for .NET with the following packages:
- `AWSSDK.Core` (v4.0.15)
- `AWSSDK.DynamoDBv2` (v4.0.15)
- `Paramore.Brighter.Inbox.DynamoDb.V4` (v10.3.0)
- `Paramore.Brighter.Outbox.DynamoDb.V4` (v10.3.0)
- `Paramore.Brighter.Locking.DynamoDb.V4` (v10.3.0)

## Configuration

### Development (Local DynamoDB)

```csharp
var dynamoDbConfig = new AmazonDynamoDBConfig
{
    Region = "us-east-1",
    ServiceUrl = "http://localhost:8000"
};

var dynamoDbClient = new AmazonDynamoDBClient(dynamoDbConfig);
```

### Production (AWS DynamoDB)

For production deployment, configure the region and use IAM roles or credentials:

```csharp
var dynamoDbConfig = new AmazonDynamoDBConfig
{
    Region = "us-east-1"  // or appropriate region
};

// Use instance profile/role-based authentication (recommended)
var dynamoDbClient = new AmazonDynamoDBClient(dynamoDbConfig);

// Or use explicit credentials
var credentials = new BasicAWSCredentials(accessKey, secretKey);
var dynamoDbClient = new AmazonDynamoDBClient(credentials, dynamoDbConfig);
```

## Validation

Check DynamoDB tables:

```bash
# Check inbox items
aws dynamodb scan --table-name brighter-inbox --endpoint-url http://localhost:8000

# Check outbox items
aws dynamodb scan --table-name brighter-outbox --endpoint-url http://localhost:8000

# Check locks
aws dynamodb scan --table-name brighter-locking --endpoint-url http://localhost:8000
```

## Rollback to MongoDB

If needed, you can rollback to MongoDB:

1. Revert code changes in `Program.cs`
2. Restore MongoDB package references in `.csproj`
3. Update configuration to use MongoDB connection string

MongoDB data remains intact during migration for rollback purposes.

## References

- [Brighter Documentation](https://paramore.readthedocs.io/)
- [DynamoDB Local Documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
- [AWS SDK for .NET v4](https://docs.aws.amazon.com/sdk-for-net/v4/developer-guide/welcome.html)