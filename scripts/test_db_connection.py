#!/usr/bin/env python3
"""
Test database connection and basic operations.
"""
import sys
import os
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test database connection and basic operations."""
    from src.utils.database import db
    
    try:
        # Test connection with a simple query
        result = db.execute_query("SELECT version()", fetch=True)
        logger.info(f"Database version: {result[0][0] if result else 'No result'}")
        
        # Test table existence
        tables = db.execute_query(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            """,
            fetch=True
        )
        
        logger.info("Available tables:")
        for table in tables:
            logger.info(f"- {table[0]}")
            
        # Test insert and select
        test_data = {
            'symbol': 'TEST',
            'price': 100.0,
            'quantity': 1.0,
            'side': 'BUY',
            'timestamp': '2025-08-24 18:00:00'
        }
        
        # Create test table if not exists
        db.execute_query("""
        CREATE TABLE IF NOT EXISTS test_connection (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            price DECIMAL(18, 8) NOT NULL,
            quantity DECIMAL(18, 8) NOT NULL,
            side VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Insert test data
        db.execute_query("""
        INSERT INTO test_connection (symbol, price, quantity, side, timestamp)
        VALUES (%(symbol)s, %(price)s, %(quantity)s, %(side)s, %(timestamp)s)
        """, test_data)
        
        # Query test data
        result = db.execute_query(
            "SELECT * FROM test_connection WHERE symbol = %s",
            ('TEST',),
            fetch=True
        )
        
        if result:
            logger.info("Successfully inserted and retrieved test data:")
            for row in result:
                logger.info(dict(zip(['id', 'symbol', 'price', 'quantity', 'side', 'timestamp', 'created_at'], row)))
        
        # Clean up
        db.execute_query("DROP TABLE IF EXISTS test_connection")
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Database test failed: {e}", exc_info=True)
        return False
    
    return True

if __name__ == "__main__":
    print("Testing database connection...")
    success = test_database_connection()
    if success:
        print("✅ Database connection test passed!")
        sys.exit(0)
    else:
        print("❌ Database connection test failed!")
        sys.exit(1)
