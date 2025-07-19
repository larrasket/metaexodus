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
      nullTransformations: 0
    };
  }

  /**
   * Transforms data for a specific table based on discovered schema
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @param {Array} data - Data to transform
   * @param {Object} enumMap - Map of enum types and their valid values
   * @returns {Promise<Array>} Transformed data
   */
  async transformTableData(connection, tableName, data, enumMap) {
    if (!data || data.length === 0) {
      return data;
    }

    // Get enum columns for this table
    const enumColumns = await schemaDiscoveryService.getEnumColumns(connection, tableName, enumMap);
    
    if (enumColumns.length === 0) {
      return data; // No enum columns to transform
    }

    // Transform data to handle enum values
    const transformedData = data.map(row => {
      const transformedRow = { ...row };

      enumColumns.forEach(col => {
        const columnName = col.column_name;
        const enumName = col.udt_name;
        const validValues = enumMap[enumName] || [];

        if (transformedRow[columnName] !== null && transformedRow[columnName] !== undefined) {
          const currentValue = transformedRow[columnName];

          // Check if current value is valid
          if (!validValues.includes(currentValue)) {
            const transformedValue = this.transformEnumValue(
              currentValue, 
              validValues, 
              tableName, 
              columnName
            );
            
            if (transformedValue !== currentValue) {
              transformedRow[columnName] = transformedValue;
              this.transformationStats.enumTransformations++;
            }
          }
        }
      });

      return transformedRow;
    });

    this.transformationStats.totalTransformations += transformedData.length;
    return transformedData;
  }

  /**
   * Transforms a single enum value to match valid values
   * @param {string} currentValue - Current enum value
   * @param {Array} validValues - Array of valid enum values
   * @param {string} tableName - Table name for logging
   * @param {string} columnName - Column name for logging
   * @returns {string|null} Transformed enum value
   */
  transformEnumValue(currentValue, validValues, tableName, columnName) {
    logger.debug(`Invalid enum value '${currentValue}' for ${tableName}.${columnName}, valid values: ${validValues.join(', ')}`);

    // Try to find a close match (case insensitive)
    const closeMatch = validValues.find(valid => 
      valid.toLowerCase() === currentValue.toLowerCase()
    );

    if (closeMatch) {
      logger.debug(`Mapped '${currentValue}' to '${closeMatch}' for ${tableName}.${columnName}`);
      return closeMatch;
    }

    // Try partial matching for common patterns
    const partialMatch = this.findPartialMatch(currentValue, validValues);
    if (partialMatch) {
      logger.debug(`Partial match '${currentValue}' to '${partialMatch}' for ${tableName}.${columnName}`);
      return partialMatch;
    }

    // Use the first valid value as default
    if (validValues.length > 0) {
      const defaultValue = validValues[0];
      logger.debug(`Defaulted '${currentValue}' to '${defaultValue}' for ${tableName}.${columnName}`);
      this.transformationStats.nullTransformations++;
      return defaultValue;
    }

    // Set to null if no valid values
    logger.warn(`No valid enum values found for ${tableName}.${columnName}, setting to null`);
    this.transformationStats.nullTransformations++;
    return null;
  }

  /**
   * Finds partial matches for enum values using common patterns
   * @param {string} currentValue - Current enum value
   * @param {Array} validValues - Array of valid enum values
   * @returns {string|null} Matched enum value or null
   */
  findPartialMatch(currentValue, validValues) {
    const current = currentValue.toLowerCase();

    // Try to find values that contain the current value or vice versa
    for (const valid of validValues) {
      const validLower = valid.toLowerCase();
      
      // Check if current value is contained in valid value
      if (validLower.includes(current) || current.includes(validLower)) {
        return valid;
      }

      // Check for common abbreviations and expansions
      if (this.isCommonAbbreviation(current, validLower)) {
        return valid;
      }
    }

    return null;
  }

  /**
   * Checks if two values are common abbreviations of each other
   * @param {string} value1 - First value
   * @param {string} value2 - Second value
   * @returns {boolean} True if they are common abbreviations
   */
  isCommonAbbreviation(value1, value2) {
    const abbreviations = {
      'activity': ['act', 'activities'],
      'individual': ['ind', 'person', 'user'],
      'group': ['grp', 'team'],
      'event': ['evt', 'event_details'],
      'news': ['news_details'],
      'notification': ['notif', 'notify']
    };

    for (const [full, abbrevs] of Object.entries(abbreviations)) {
      if ((value1 === full && abbrevs.includes(value2)) ||
          (value2 === full && abbrevs.includes(value1))) {
        return true;
      }
    }

    return false;
  }

  /**
   * Validates transformed data against schema constraints
   * @param {Object} connection - Database connection
   * @param {string} tableName - Name of the table
   * @param {Array} data - Data to validate
   * @returns {Promise<Object>} Validation result
   */
  async validateTransformedData(connection, tableName, data) {
    if (!data || data.length === 0) {
      return { valid: true, issues: [] };
    }

    const schema = await schemaDiscoveryService.discoverTableSchema(connection, tableName);
    const issues = [];

    // Sample validation on first few rows
    const sampleSize = Math.min(data.length, 10);
    
    for (let i = 0; i < sampleSize; i++) {
      const row = data[i];
      
      for (const column of schema) {
        const value = row[column.column_name];
        
        // Check for null values in non-nullable columns
        if (value === null || value === undefined) {
          if (column.is_nullable === 'NO' && !column.column_default) {
            issues.push({
              type: 'null_constraint_violation',
              table: tableName,
              column: column.column_name,
              row: i,
              message: `Null value in non-nullable column '${column.column_name}'`
            });
          }
        }
      }
    }

    return {
      valid: issues.length === 0,
      issues,
      sampledRows: sampleSize
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
      nullTransformations: 0
    };
  }
}

const dataTransformationService = new DataTransformationService();

export { DataTransformationService, dataTransformationService };