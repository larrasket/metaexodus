import { logger } from '../utils/logger.js';

/**
 * Service for discovering database schema information
 */
class SchemaDiscoveryService {
  constructor() {
    this.schemaCache = new Map();
  }

  /**
   * Discovers all enum types and their valid values from PostgreSQL
   * @param {Object} connection - Database connection
   * @returns {Promise<Object>} Map of enum types to their valid values
   */
  async discoverEnumValues(connection) {
    try {
      const query = `
        SELECT 
          t.typname as enum_name,
          e.enumlabel as enum_value
        FROM pg_type t 
        JOIN pg_enum e ON t.oid = e.enumtypid  
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public'
        ORDER BY t.typname, e.enumsortorder;
      `;

      const result = await connection.query(query);
      const enumMap = {};

      result.rows.forEach((row) => {
        if (!enumMap[row.enum_name]) {
          enumMap[row.enum_name] = [];
        }
        enumMap[row.enum_name].push(row.enum_value);
      });

      // Cache the results
      this.schemaCache.set('enums', enumMap);

      return enumMap;
    } catch (error) {
      logger.warn(`Could not discover enum values: ${error.message}`);
      return {};
    }
  }

  /**
   * Discovers table schema information including column types and constraints
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @returns {Promise<Array>} Array of column information
   */
  async discoverTableSchema(connection, tableName) {
    const cacheKey = `table_${tableName}`;

    if (this.schemaCache.has(cacheKey)) {
      return this.schemaCache.get(cacheKey);
    }

    try {
      const query = `
        SELECT 
          c.column_name,
          c.data_type,
          c.udt_name,
          c.is_nullable,
          c.column_default,
          c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_name = $1 
        AND c.table_schema = 'public'
        ORDER BY c.ordinal_position;
      `;

      const result = await connection.query(query, [tableName]);

      // Cache the results
      this.schemaCache.set(cacheKey, result.rows);

      return result.rows;
    } catch (error) {
      logger.warn(
        `Could not discover schema for table ${tableName}: ${error.message}`
      );
      return [];
    }
  }

  /**
   * Discovers foreign key constraints for a table
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @returns {Promise<Array>} Array of foreign key constraint information
   */
  async discoverForeignKeys(connection, tableName) {
    const cacheKey = `fk_${tableName}`;

    if (this.schemaCache.has(cacheKey)) {
      return this.schemaCache.get(cacheKey);
    }

    try {
      const query = `
        SELECT 
          tc.constraint_name,
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu 
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = $1
        AND tc.table_schema = 'public';
      `;

      const result = await connection.query(query, [tableName]);

      // Cache the results
      this.schemaCache.set(cacheKey, result.rows);

      return result.rows;
    } catch (error) {
      logger.warn(
        `Could not discover foreign keys for table ${tableName}: ${error.message}`
      );
      return [];
    }
  }

  /**
   * Discovers all table names in the database
   * @param {Object} connection - Database connection
   * @returns {Promise<Array>} Array of table names
   */
  async discoverTables(connection) {
    if (this.schemaCache.has('tables')) {
      return this.schemaCache.get('tables');
    }

    try {
      const query = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `;

      const result = await connection.query(query);
      const tableNames = result.rows.map((row) => row.table_name);

      // Cache the results
      this.schemaCache.set('tables', tableNames);

      return tableNames;
    } catch (error) {
      logger.warn(`Could not discover tables: ${error.message}`);
      return [];
    }
  }

  /**
   * Gets comprehensive schema information for a table
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @returns {Promise<Object>} Complete table schema information
   */
  async getTableSchemaInfo(connection, tableName) {
    const [columns, foreignKeys] = await Promise.all([
      this.discoverTableSchema(connection, tableName),
      this.discoverForeignKeys(connection, tableName)
    ]);

    return {
      tableName,
      columns,
      foreignKeys,
      enumColumns: columns.filter(
        (col) => col.udt_name && this.schemaCache.get('enums')?.[col.udt_name]
      )
    };
  }

  /**
   * Clears the schema cache
   * @param {string} key - Optional specific key to clear
   */
  clearCache(key = null) {
    if (key) {
      this.schemaCache.delete(key);
    } else {
      this.schemaCache.clear();
    }
  }

  /**
   * Gets cache statistics
   * @returns {Object} Cache statistics
   */
  getCacheStats() {
    return {
      cacheSize: this.schemaCache.size,
      cachedKeys: Array.from(this.schemaCache.keys())
    };
  }
}

const schemaDiscoveryService = new SchemaDiscoveryService();

export { SchemaDiscoveryService, schemaDiscoveryService };
