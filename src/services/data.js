import { logger } from '../utils/logger.js';
import { connectionService } from './connection.js';

class DataService {
  constructor() {
    this.batchSize = parseInt(process.env.DB_BATCH_SIZE) || 1000;
    this.isInitialized = false;
  }

  async initialize() {
    try {
      if (!connectionService.isInitialized) {
        const initResult = await connectionService.initialize();
        if (!initResult.success) {
          return {
            success: false,
            error: 'Failed to initialize connection service',
            details: initResult.details
          };
        }
      }

      this.isInitialized = true;
      return {
        success: true,
        message: 'Data service initialized successfully'
      };
    } catch (error) {
      return {
        success: false,
        error: 'Failed to initialize data service',
        details: [error.message]
      };
    }
  }

  async getTableNames(connection) {
    try {
      const query = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
      `;

      const result = await connection.query(query);
      return result.rows.map((row) => row.table_name);
    } catch (error) {
      throw new Error(`Failed to get table names: ${error.message}`);
    }
  }

  async getTableDependencies(connection) {
    try {
      const query = `
        SELECT 
          tc.table_name as dependent_table,
          ccu.table_name as referenced_table
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu 
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public';
      `;

      const result = await connection.query(query);
      const dependencies = {};

      result.rows.forEach((row) => {
        if (!dependencies[row.dependent_table]) {
          dependencies[row.dependent_table] = [];
        }
        dependencies[row.dependent_table].push(row.referenced_table);
      });

      return dependencies;
    } catch (error) {
      throw new Error(`Failed to get table dependencies: ${error.message}`);
    }
  }

  sortTablesByDependencies(tableNames, dependencies) {
    const sorted = [];
    const visited = new Set();
    const visiting = new Set();

    const visit = (tableName) => {
      if (visiting.has(tableName)) {
        return;
      }

      if (visited.has(tableName)) {
        return;
      }

      visiting.add(tableName);

      const deps = dependencies[tableName] || [];
      deps.forEach((dep) => {
        if (tableNames.includes(dep)) {
          visit(dep);
        }
      });

      visiting.delete(tableName);
      visited.add(tableName);
      sorted.push(tableName);
    };

    tableNames.forEach((tableName) => {
      if (!visited.has(tableName)) {
        visit(tableName);
      }
    });

    return sorted;
  }

  async getTableRowCount(connection, tableName) {
    try {
      const query = `SELECT COUNT(*) as count FROM "${tableName}";`;
      const result = await connection.query(query);
      return parseInt(result.rows[0].count);
    } catch (error) {
      throw new Error(
        `Failed to get row count for table ${tableName}: ${error.message}`
      );
    }
  }

  async extractTableData(connection, tableName, options = {}) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    const { offset = 0, limit = this.batchSize } = options;

    try {
      const totalRows = await this.getTableRowCount(connection, tableName);

      if (totalRows === 0) {
        return {
          tableName,
          data: [],
          totalRows: 0,
          extractedRows: 0,
          hasMore: false,
          nextOffset: 0
        };
      }

      const query = `SELECT * FROM "${tableName}" ORDER BY 1 LIMIT $1 OFFSET $2;`;
      const result = await connection.query(query, [limit, offset]);

      const extractedRows = result.rows.length;
      const hasMore = offset + extractedRows < totalRows;
      const nextOffset = hasMore ? offset + extractedRows : 0;

      return {
        tableName,
        data: result.rows,
        totalRows,
        extractedRows,
        hasMore,
        nextOffset,
        columns: result.fields ? result.fields.map((field) => field.name) : []
      };
    } catch (error) {
      throw new Error(
        `Failed to extract data from table ${tableName}: ${error.message}`
      );
    }
  }

  async extractAllTableData(connection, tableName, onBatch = null) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    try {
      let offset = 0;
      let totalExtracted = 0;
      let allData = [];
      let hasMore = true;
      let totalRows = 0;

      while (hasMore) {
        const batchResult = await this.extractTableData(connection, tableName, {
          offset,
          limit: this.batchSize
        });

        totalRows = batchResult.totalRows;
        allData = allData.concat(batchResult.data);
        totalExtracted += batchResult.extractedRows;
        hasMore = batchResult.hasMore;
        offset = batchResult.nextOffset;

        if (onBatch && typeof onBatch === 'function') {
          await onBatch({
            tableName,
            batchData: batchResult.data,
            batchNumber: Math.floor(offset / this.batchSize),
            totalExtracted,
            totalRows,
            progress: (totalExtracted / totalRows) * 100
          });
        }
      }

      return {
        tableName,
        data: allData,
        totalRows,
        extractedRows: totalExtracted,
        batchCount: Math.ceil(totalExtracted / this.batchSize)
      };
    } catch (error) {
      throw new Error(
        `Failed to extract all data from table ${tableName}: ${error.message}`
      );
    }
  }

  async extractAllData(
    connection,
    onTableStart = null,
    onTableComplete = null,
    onBatch = null
  ) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    try {
      const tableNames = await this.getTableNames(connection);

      if (tableNames.length === 0) {
        return {
          success: true,
          tables: [],
          totalTables: 0,
          totalRows: 0,
          message: 'No tables found to extract'
        };
      }

      const dependencies = await this.getTableDependencies(connection);
      const sortedTables = this.sortTablesByDependencies(
        tableNames,
        dependencies
      );

      const results = [];
      let totalRows = 0;
      const errors = [];

      for (const tableName of sortedTables) {
        try {
          if (onTableStart && typeof onTableStart === 'function') {
            await onTableStart({
              tableName,
              tableIndex: results.length,
              totalTables: sortedTables.length
            });
          }

          const tableResult = await this.extractAllTableData(
            connection,
            tableName,
            onBatch
          );

          results.push(tableResult);
          totalRows += tableResult.totalRows;

          if (onTableComplete && typeof onTableComplete === 'function') {
            await onTableComplete({
              tableName,
              tableIndex: results.length - 1,
              totalTables: sortedTables.length,
              extractedRows: tableResult.extractedRows,
              totalRows: tableResult.totalRows
            });
          }
        } catch (error) {
          const errorInfo = {
            tableName,
            error: error.message,
            timestamp: new Date().toISOString()
          };
          errors.push(errorInfo);

          logger.error(`Error extracting data from table ${tableName}`, error);
        }
      }

      return {
        success: errors.length === 0,
        tables: results,
        totalTables: sortedTables.length,
        totalRows,
        errors,
        tableOrder: sortedTables,
        dependencies
      };
    } catch (error) {
      throw new Error(`Failed to extract all data: ${error.message}`);
    }
  }

  getExtractionStats() {
    return {
      batchSize: this.batchSize,
      initialized: this.isInitialized
    };
  }

  configureBatchSize(batchSize) {
    if (batchSize > 0) {
      this.batchSize = batchSize;
    }
  }

  async clearTableData(connection, tableName) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    try {
      const result = await connection.query(`DELETE FROM "${tableName}";`);
      return {
        tableName,
        deletedRows: result.rowCount || 0,
        success: true
      };
    } catch (error) {
      throw new Error(`Failed to clear table ${tableName}: ${error.message}`);
    }
  }

  async getTableColumns(connection, tableName) {
    try {
      const query = `
        SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND table_schema = 'public'
        ORDER BY ordinal_position;
      `;

      const result = await connection.query(query, [tableName]);
      return result.rows;
    } catch (error) {
      throw new Error(
        `Failed to get columns for table ${tableName}: ${error.message}`
      );
    }
  }

  async insertTableData(connection, tableName, data, options = {}) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    if (!data || data.length === 0) {
      return {
        tableName,
        insertedRows: 0,
        totalRows: 0,
        success: true,
        batches: 0
      };
    }

    const {
      clearFirst = false,
      onConflict = 'error',
      batchSize = this.batchSize
    } = options;

    try {
      let totalInserted = 0;
      let batchCount = 0;
      const errors = [];

      if (clearFirst) {
        await this.clearTableData(connection, tableName);
      }

      const columns = await this.getTableColumns(connection, tableName);
      const columnNames = columns.map((col) => col.column_name);

      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);
        batchCount++;

        try {
          const batchResult = await this.insertBatch(
            connection,
            tableName,
            batch,
            columnNames,
            onConflict
          );
          totalInserted += batchResult.insertedRows;
        } catch (error) {
          errors.push({
            batchNumber: batchCount,
            batchSize: batch.length,
            error: error.message,
            timestamp: new Date().toISOString()
          });

          if (
            error.message.includes('does not exist') ||
            error.message.includes('permission')
          ) {
            throw error;
          }
        }
      }

      return {
        tableName,
        insertedRows: totalInserted,
        totalRows: data.length,
        success: errors.length === 0,
        batches: batchCount,
        errors
      };
    } catch (error) {
      throw new Error(
        `Failed to insert data into table ${tableName}: ${error.message}`
      );
    }
  }

  async insertBatch(connection, tableName, batch, columnNames, onConflict) {
    if (batch.length === 0) {
      return { insertedRows: 0 };
    }

    // Use union of keys from all rows in the batch to avoid missing columns
    const dataKeySet = new Set();
    batch.forEach((row) => {
      Object.keys(row || {}).forEach((k) => dataKeySet.add(k));
    });
    const dataKeys = Array.from(dataKeySet);
    const validColumns = columnNames.filter((col) => dataKeys.includes(col));

    if (validColumns.length === 0) {
      throw new Error(`No valid columns found for table ${tableName}`);
    }

    const placeholders = batch
      .map((_, rowIndex) => {
        const rowPlaceholders = validColumns.map(
          (_, colIndex) => `$${rowIndex * validColumns.length + colIndex + 1}`
        );
        return `(${rowPlaceholders.join(', ')})`;
      })
      .join(', ');

    const columnsList = validColumns.map((col) => `"${col}"`).join(', ');

    let query = `INSERT INTO "${tableName}" (${columnsList}) VALUES ${placeholders}`;

    if (onConflict === 'skip') {
      query += ' ON CONFLICT DO NOTHING';
    } else if (onConflict === 'update') {
      const updateSet = validColumns
        .map((col) => `"${col}" = EXCLUDED."${col}"`)
        .join(', ');
      query += ` ON CONFLICT DO UPDATE SET ${updateSet}`;
    }

    const values = [];
    batch.forEach((row) => {
      validColumns.forEach((col) => {
        let value = row[col];
        if (value === undefined) {
          value = null;
        }
        if (value === '') {
          value = null;
        }
        // Normalize arrays/objects to JSON strings to fit JSONB columns
        if (Array.isArray(value) || (value && typeof value === 'object')) {
          value = JSON.stringify(value);
        }
        // If a string looks like JSON array/object, pass as-is for JSONB
        if (typeof value === 'string') {
          const t = value.trim();
          if (
            (t.startsWith('[') && t.endsWith(']')) ||
            (t.startsWith('{') && t.endsWith('}'))
          ) {
            value = t;
          }
        }
        values.push(value);
      });
    });

    // Sanity check: values length must match batch size * columns
    const expectedValues = batch.length * validColumns.length;
    if (values.length !== expectedValues) {
      logger.error(
        `Values length mismatch for table ${tableName}: expected ${expectedValues} values but got ${values.length}`
      );
      logger.debug('Constructed query:', { query, expectedValues, valuesLength: values.length, validColumns, sampleRow: batch[0] });
      throw new Error(
        `Batch construction error: expected ${expectedValues} values but got ${values.length}`
      );
    }

    try {
      const result = await connection.query(query, values);
      return {
        insertedRows: result.rowCount || 0
      };
    } catch (error) {
      // Provide more detailed error information
      const errorMessage = error.message;
      const errorCode = error.code;

      // Log the problematic data for debugging
      logger.debug(`Batch insertion failed for table ${tableName}:`, {
        error: errorMessage,
        code: errorCode,
        batchSize: batch.length,
        columns: validColumns,
        sampleRow: batch[0]
      });

      // If batch insertion fails, attempt to insert rows individually to isolate bad rows.
      logger.warn(
        `Batch insert failed for ${tableName} (batchSize=${batch.length}). Falling back to per-row insert to isolate errors.`
      );

      const rowErrors = [];
      let rowsInserted = 0;

      for (let r = 0; r < batch.length; r++) {
        const singleRow = batch[r];
        const singlePlaceholders = validColumns.map((_, idx) => `$${idx + 1}`).join(', ');
        const singleQuery = `INSERT INTO "${tableName}" (${columnsList}) VALUES (${singlePlaceholders})` +
          (onConflict === 'skip' ? ' ON CONFLICT DO NOTHING' : onConflict === 'update' ? ` ON CONFLICT DO UPDATE SET ${validColumns.map((col) => `"${col}" = EXCLUDED."${col}"`).join(', ')}` : '');

        const singleValues = validColumns.map((col) => {
          let value = singleRow[col];
          if (value === undefined) {
            value = null;
          }
          if (value === '') {
            value = null;
          }
          if (Array.isArray(value) || (value && typeof value === 'object')) {
            value = JSON.stringify(value);
          }
          if (typeof value === 'string') {
            const t = value.trim();
            if ((t.startsWith('[') && t.endsWith(']')) || (t.startsWith('{') && t.endsWith('}'))) {
              value = t;
            }
          }
          return value;
        });

        try {
          const res = await connection.query(singleQuery, singleValues);
          rowsInserted += res.rowCount || 0;
        } catch (singleErr) {
          rowErrors.push({ rowIndex: r, error: singleErr.message, sampleRow: singleRow });
          logger.debug(`Row insert failed for ${tableName} row ${r}: ${singleErr.message}`, { sampleRow: singleRow });
        }
      }

      if (rowErrors.length > 0) {
        logger.error(`Per-row insertion reported ${rowErrors.length} failing rows for ${tableName}`);
      }

      return { insertedRows: rowsInserted, rowErrors };
    }
  }

  async syncTableData(
    sourceConnection,
    targetConnection,
    tableName,
    options = {}
  ) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    const {
      clearTarget = true,
      onConflict = 'error',
      onProgress = null
    } = options;

    try {
      const extractResult = await this.extractAllTableData(
        sourceConnection,
        tableName,
        onProgress
      );

      if (extractResult.totalRows === 0) {
        return {
          tableName,
          sourceRows: 0,
          insertedRows: 0,
          success: true,
          message: 'No data to synchronize'
        };
      }

      const insertResult = await this.insertTableData(
        targetConnection,
        tableName,
        extractResult.data,
        {
          clearFirst: clearTarget,
          onConflict
        }
      );

      return {
        tableName,
        sourceRows: extractResult.totalRows,
        insertedRows: insertResult.insertedRows,
        success: insertResult.success,
        errors: insertResult.errors || [],
        batches: insertResult.batches
      };
    } catch (error) {
      throw new Error(`Failed to sync table ${tableName}: ${error.message}`);
    }
  }

  async syncAllData(sourceConnection, targetConnection, options = {}) {
    if (!this.isInitialized) {
      throw new Error('Data service not initialized. Call initialize() first.');
    }

    const {
      clearTarget = true,
      onConflict = 'error',
      onTableStart = null,
      onTableComplete = null,
      onProgress = null
    } = options;

    try {
      const tableNames = await this.getTableNames(sourceConnection);

      if (tableNames.length === 0) {
        return {
          success: true,
          tables: [],
          totalTables: 0,
          totalSourceRows: 0,
          totalInsertedRows: 0,
          message: 'No tables found to synchronize'
        };
      }

      const dependencies = await this.getTableDependencies(sourceConnection);
      const sortedTables = this.sortTablesByDependencies(
        tableNames,
        dependencies
      );

      const results = [];
      let totalSourceRows = 0;
      let totalInsertedRows = 0;
      const errors = [];

      for (const tableName of sortedTables) {
        try {
          if (onTableStart && typeof onTableStart === 'function') {
            await onTableStart({
              tableName,
              tableIndex: results.length,
              totalTables: sortedTables.length
            });
          }

          const syncResult = await this.syncTableData(
            sourceConnection,
            targetConnection,
            tableName,
            {
              clearTarget,
              onConflict,
              onProgress
            }
          );

          results.push(syncResult);
          totalSourceRows += syncResult.sourceRows;
          totalInsertedRows += syncResult.insertedRows;

          if (onTableComplete && typeof onTableComplete === 'function') {
            await onTableComplete({
              tableName,
              tableIndex: results.length - 1,
              totalTables: sortedTables.length,
              sourceRows: syncResult.sourceRows,
              insertedRows: syncResult.insertedRows,
              success: syncResult.success
            });
          }
        } catch (error) {
          const errorInfo = {
            tableName,
            error: error.message,
            timestamp: new Date().toISOString()
          };
          errors.push(errorInfo);

          logger.error(`Error synchronizing table ${tableName}`, error);
        }
      }

      return {
        success: errors.length === 0,
        tables: results,
        totalTables: sortedTables.length,
        totalSourceRows,
        totalInsertedRows,
        errors,
        tableOrder: sortedTables
      };
    } catch (error) {
      throw new Error(`Failed to synchronize all data: ${error.message}`);
    }
  }
}

const dataService = new DataService();

export { DataService, dataService };
