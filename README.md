# metaflake

An ID generator for snowflake nodes.

## Dependency

Create Amazon DynamoDB table as following terraform configuration.
```
resource "aws_dynamodb_table" "metaflake_table" {
  name           = "metaflake_table"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "i"

  attribute {
    name = "i"
    type = "N"
  }

  ttl {
    attribute_name = "t"
    enabled = true
  }
}
```

## How to use

```
# Run metaflake with TABLE env,
TABLE=metaflake_table metaflake &

# fetch ID,
ID=$(curl -sf http://localhost:8000/ --retry 20 --retry-connrefused --retry-delay 1)

# and use.
echo $ID

# When you want to release the ID, just kill metaflake process.
pkill metaflake
```
