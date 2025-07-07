```powershell
# Ensure PostgreSQL client is installed and accessible
$psql = "psql"
if (-Not (Get-Command $psql -ErrorAction SilentlyContinue)) {
    Write-Error "psql not found. Please install PostgreSQL from https://www.postgresql.org/download/windows/ and add it to PATH."
    exit 1
}

# Update db_config.yaml with default credentials matching docker-compose
@"
postgresql:
  host: 'localhost'
  port: 5432
  database: 'trading_bot'
  user: 'postgres'
  password: 'postgres'
"@ | Out-File -FilePath config/db_config.yaml -Encoding utf8

# Create database if it doesn't exist
try {
    & $psql -U postgres -c "CREATE DATABASE trading_bot;" 2>$null
    Write-Host "Database 'trading_bot' created or already exists"
} catch {
    Write-Error "Failed to create database: $_"
    exit 1
}

# Apply schema
try {
    & $psql -h localhost -U postgres -d trading_bot -f src/database/schema.sql
    Write-Host "Database schema applied successfully"
} catch {
    Write-Error "Failed to apply schema: $_"
    exit 1
}
```
