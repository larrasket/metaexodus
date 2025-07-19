import { logger } from '../utils/logger.js';
import { schemaDiscoveryService } from './schemaDiscovery.js';

/**
 * Service for transforming data to match target database schema
 */
class DataTransformationService {
  constructor() {
    this.transformationStats = {
      totalTransformations: 0,
      enumTransformations: 0,
      nullTransformations: 0,
      typeConversions: 0
    };
  }

  /**
   * Transforms table data to match PostgreSQL schema
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @param {Array} data - Raw data from source
   * @param {Object} enumMap - Map of enum types to valid values
   * @returns {Promise<Array>} Transformed data
   */
  async transformTableData(connection, tableName, data, enumMap) {
    if (!data || data.length === 0) {
      return data;
    }

    // Get table schema information
    const schema = await schemaDiscoveryService.discoverTableSchema(connection, tableName);
    const enumColumns = schema.filter(col => col.udt_name && enumMap[col.udt_name]);
    
    if (enumColumns.length === 0) {
      return data; // No transformations needed
    }

    // Transform each row
    const transformedData = data.map(row => this.transformRow(row, enumColumns, enumMap, tableName));
    
    logger.debug(`Transformed ${transformedData.length} rows for table ${tableName}`);
    return transformedData;
  }

  /**
   * Transforms a single row of data
   * @param {Object} row - Data row
   * @param {Array} enumColumns - Columns with enum types
   * @param {Object} enumMap - Map of enum types to valid values
   * @param {string} tableName - Name of the table (for logging)
   * @returns {Object} Transformed row
   */
  transformRow(row, enumColumns, enumMap, tableName) {
    const transformedRow = { ...row };
    
    enumColumns.forEach(col => {
      const columnName = col.column_name;
      const enumName = col.udt_name;
      const validValues = enumMap[enumName] || [];
      
      if (transformedRow[columnName] !== null && transformedRow[columnName] !== undefined) {
        const currentValue = transformedRow[columnName];
        
        // Check if current value is valid
        if (!validValues.includes(currentValue)) {
          const transformedValue = this.transformEnumValue(currentValue, validValues, tableName, columnName);
          transformedRow[columnName] = transformedValue;
          this.transformationStats.enumTransformations++;
        }
      }
    });
    
    this.transformationStats.totalTransformations++;
    return transformedRow;
  }

  /**
   * Transforms an enum value to match valid PostgreSQL enum values
   * @param {string} currentValue - Current enum value
   * @param {Array} validValues - Valid enum values
   * @param {string} tableName - Table name (for logging)
   * @param {string} columnName - Column name (for logging)
   * @returns {string|null} Transformed enum value
   */
  transformEnumValue(currentValue, validValues, tableName, columnName) {
    // Try exact match (case sensitive)
    if (validValues.includes(currentValue)) {
      return currentValue;
    }

    // Try case insensitive match
    const caseInsensitiveMatch = validValues.find(valid => 
      valid.toLowerCase() === currentValue.toLowerCase()
    );
    
    if (caseInsensitiveMatch) {
      logger.debug(`Case-insensitive match: '${currentValue}' -> '${caseInsensitiveMatch}' for ${tableName}.${columnName}`);
      return caseInsensitiveMatch;
    }

    // Try partial match (contains)
    const partialMatch = validValues.find(valid => 
      valid.toLowerCase().includes(currentValue.toLowerCase()) ||
      currentValue.toLowerCase().includes(valid.toLowerCase())
    );
    
    if (partialMatch) {
      logger.debug(`Partial match: '${currentValue}' -> '${partialMatch}' for ${tableName}.${columnName}`);
      return partialMatch;
    }

    // Try common enum value mappings
    const commonMappings = this.getCommonEnumMappings();
    const mappedValue = commonMappings[currentValue.toLowerCase()];
    
    if (mappedValue && validValues.includes(mappedValue)) {
      logger.debug(`Common mapping: '${currentValue}' -> '${mappedValue}' for ${tableName}.${columnName}`);
      return mappedValue;
    }

    // Default to first valid value if available
    if (validValues.length > 0) {
      logger.debug(`Default mapping: '${currentValue}' -> '${validValues[0]}' for ${tableName}.${columnName}`);
      return validValues[0];
    }

    // Set to null if no valid values
    logger.warn(`No valid enum mapping found for '${currentValue}' in ${tableName}.${columnName}, setting to null`);
    this.transformationStats.nullTransformations++;
    return null;
  }

  /**
   * Gets common enum value mappings for automatic transformation
   * @returns {Object} Map of common enum value transformations
   */
  getCommonEnumMappings() {
    return {
      // Common activity/target type mappings
      'activity': 'INDIVIDUAL',
      'user': 'INDIVIDUAL',
      'individual': 'INDIVIDUAL',
      'group': 'GROUP',
      'all': 'ALL',
      'everyone': 'ALL',
      
      // Common page type mappings
      'event_details': 'EVENT',
      'event': 'EVENT',
      'news_details': 'NEWS',
      'news': 'NEWS',
      'profile': 'PROFILE',
      'home': 'HOME',
      'dashboard': 'HOME',
      
      // Common status mappings
      'active': 'ACTIVE',
      'inactive': 'INACTIVE',
      'enabled': 'ACTIVE',
      'disabled': 'INACTIVE',
      'on': 'ACTIVE',
      'off': 'INACTIVE',
      
      // Common boolean-like mappings
      'true': 'TRUE',
      'false': 'FALSE',
      'yes': 'TRUE',
      'no': 'FALSE',
      '1': 'TRUE',
      '0': 'FALSE'
    };
  }

  /**
   * Transforms data types for better PostgreSQL compatibility
   * @param {any} value - Value to transform
   * @param {string} targetType - Target PostgreSQL type
   * @returns {any} Transformed value
   */
  transformDataType(value, targetType) {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      switch (targetType.toLowerCase()) {
        case 'integer':
        case 'bigint':
        case 'smallint':
          const intValue = parseInt(value);
          return isNaN(intValue) ? null : intValue;
          
        case 'numeric':
        case 'decimal':
        case 'real':
        case 'double precision':
          const numValue = parseFloat(value);
          return isNaN(numValue) ? null : numValue;
          
        case 'boolean':
          if (typeof value === 'boolean') return value;
          const strValue = String(value).toLowerCase();
          return ['true', '1', 'yes', 'on', 't', 'y'].includes(strValue);
          
        case 'date':
        case 'timestamp':
        case 'timestamptz':
          if (value instanceof Date) return value;
          const dateValue = new Date(value);
          return isNaN(dateValue.getTime()) ? null : dateValue;
          
        case 'json':
        case 'jsonb':
          if (typeof value === 'object') return JSON.stringify(value);
          return value;
          
        default:
          // For text, varchar, and other string types
          return String(value);
      }
    } catch (error) {
      logger.warn(`Type conversion failed for value '${value}' to type '${targetType}': ${error.message}`);
      this.transformationStats.typeConversions++;
      return null;
    }
  }

  /**
   * Validates transformed data against schema constraints
   * @param {Array} data - Transformed data
   * @param {Array} schema - Table schema information
   * @returns {Object} Validation result
   */
  validateTransformedData(data, schema) {
    const issues = [];
    const requiredColumns = schema.filter(col => col.is_nullable === 'NO' && !col.column_default);
    
    data.forEach((row, index) => {
      requiredColumns.forEach(col => {
        if (row[col.column_name] === null || row[col.column_name] === undefined) {
          issues.push({
            type: 'null_constraint_violation',
            row: index,
            column: col.column_name,
            message: `Required column '${col.column_name}' cannot be null`
          });
        }
      });
    });

    return {
      valid: issues.length === 0,
      issues,
      validatedRows: data.length
    };
  }

  /**
   * Gets transformation statistics
   * @returns {Object} Transformation statistics
   */
  getTransformationStats() {
    return { ...this.transformationStats };
  }

  /**
   * Resets transformation statistics
   */
  resetStats() {
    this.transformationStats = {
      totalTransformations: 0,
      enumTransformations: 0,
      nullTransformations: 0,
      typeConversions: 0
    };
  }
}

const dataTransformationService = new DataTransformationService();

export { DataTransformationService, dataTransformationService };