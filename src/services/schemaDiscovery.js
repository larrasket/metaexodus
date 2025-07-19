import { logger } from '../utils/logger.js';

/**
 * Service for discovering and analyzing database schema information
 */
class SchemaDiscoveryService {
  constructor() {
    this.enumCache = new Map();
    this.schemaCache = new Map();
  }

  /**
   * Discovers all enum types and their valid values from PostgreSQL
   * @param {Object} connection - Database connection
   * @returns {Promise<Object>} Map of enum names to their valid values
   */
  async discoverEnumValues(connection) {
    const cacheKey = 'enums';
    if (this.enumCache.has(cacheKey)) {
      return this.enumCache.get(cacheKey);
    }

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

      result.rows.forEach(row => {
        if (!enumMap[row.enum_name]) {
          enumMap[row.enum_name] = [];
        }
        enumMap[row.enum_name].push(row.enum_value);
      });

      this.enumCache.set(cacheKey, enumMap);
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
    if (this.schemaCache.has(tableName)) {
      return this.schemaCache.get(tableName);
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
      this.schemaCache.set(tableName, result.rows);
      return result.rows;
    } catch (error) {
      logger.warn(`Could not discover schema for table ${tableName}: ${error.message}`);
      return [];
    }
  }

  /**
   * Gets enum columns for a specific table
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @param {Object} enumMap - Map of enum types
   * @returns {Promise<Array>} Array of enum column information
   */
  async getEnumColumns(connection, tableName, enumMap) {
    const schema = await this.discoverTableSchema(connection, tableName);
    return schema.filter(col => col.udt_name && enumMap[col.udt_name]);
  }

  /**
   * Clears the schema cache
   * @param {string} tableName - Optional table name to clear specific cache
   */
  clearCache(tableName = null) {
    if (tableName) {
      this.schemaCache.delete(tableName);
    } else {
      this.schemaCache.clear();
      this.enumCache.clear();
    }
  }

  /**
   * Gets cache statistics
   * @returns {Object} Cache statistics
   */
  getCacheStats() {
    return {
      enumCacheSize: this.enumCache.size,
      schemaCacheSize: this.schemaCache.size,
      cachedTables: Array.from(this.schemaCache.keys())
    };
  }
}

const schemaDiscoveryService = new SchemaDiscoveryService();

export { SchemaDiscoveryService, schemaDiscoveryService };