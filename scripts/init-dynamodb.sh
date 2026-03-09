#!/bin/bash
# Initialize DynamoDB tables for Brighter with correct schema and TTL

set -e

ENDPOINT_URL="http://localhost:8000"

echo "Creating DynamoDB tables for Brighter..."

# Inbox Table - Composite key for duplicate detection
aws dynamodb create-table \
    --table-name brighter_inbox \
    --attribute-definitions \
        AttributeName=CommandId,AttributeType=S \
        AttributeName=ContextKey,AttributeType=S \
    --key-schema \
        AttributeName=CommandId,KeyType=HASH \
        AttributeName=ContextKey,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url $ENDPOINT_URL || echo "Table brighter_inbox already exists"

# Enable TTL on inbox (1 day) - using ExpiresAt field (Unix timestamp in ticks)
aws dynamodb update-time-to-live \
    --table-name brighter_inbox \
    --time-to-live-specification Enabled=true,AttributeName=ExpiresAt \
    --endpoint-url $ENDPOINT_URL || echo "TTL already enabled on brighter_inbox"

# Outbox Table - Hash key with GSIs for querying
aws dynamodb create-table \
    --table-name brighter_outbox \
    --attribute-definitions \
        AttributeName=MessageId,AttributeType=S \
        AttributeName=TopicShard,AttributeType=S \
        AttributeName=OutstandingCreatedTime,AttributeType=N \
        AttributeName=DeliveryTime,AttributeType=N \
    --key-schema \
        AttributeName=MessageId,KeyType=HASH \
    --global-secondary-indexes \
        "IndexName=Outstanding,KeySchema=[{AttributeName=TopicShard,KeyType=HASH},{AttributeName=OutstandingCreatedTime,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
        "IndexName=OutstandingAllTopics,KeySchema=[{AttributeName=OutstandingCreatedTime,KeyType=HASH},{AttributeName=MessageId,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
        "IndexName=Delivered,KeySchema=[{AttributeName=TopicShard,KeyType=HASH},{AttributeName=DeliveryTime,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
        "IndexName=DeliveredAllTopics,KeySchema=[{AttributeName=DeliveryTime,KeyType=HASH},{AttributeName=MessageId,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url $ENDPOINT_URL || echo "Table brighter_outbox already exists"

# Enable TTL on outbox (1 day)
aws dynamodb update-time-to-live \
    --table-name brighter_outbox \
    --time-to-live-specification Enabled=true,AttributeName=ExpiresAt \
    --endpoint-url $ENDPOINT_URL || echo "TTL already enabled on brighter_outbox"

# Locking Table - Simple hash key
aws dynamodb create-table \
    --table-name brighter_distributed_lock \
    --attribute-definitions AttributeName=ResourceId,AttributeType=S \
    --key-schema AttributeName=ResourceId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url $ENDPOINT_URL || echo "Table brighter_distributed_lock already exists"

# Enable TTL on locking table (1 day)
aws dynamodb update-time-to-live \
    --table-name brighter_distributed_lock \
    --time-to-live-specification Enabled=true,AttributeName=LeaseExpiry \
    --endpoint-url $ENDPOINT_URL || echo "TTL already enabled on brighter_distributed_lock"

echo "DynamoDB tables created successfully!"
echo ""
echo "Table Status:"
aws dynamodb describe-table --table-name brighter_inbox --endpoint-url $ENDPOINT_URL --query 'Table.TableStatus' --output text
aws dynamodb describe-table --table-name brighter_outbox --endpoint-url $ENDPOINT_URL --query 'Table.TableStatus' --output text
aws dynamodb describe-table --table-name brighter_distributed_lock --endpoint-url $ENDPOINT_URL --query 'Table.TableStatus' --output text
