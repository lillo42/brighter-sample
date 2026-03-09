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
