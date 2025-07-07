#!/bin/bash
psql -h localhost -U postgres -d trading_bot -f src/database/schema.sql

