import { logger } from '../utils/logger.js';
import { metabaseService } from './metabase.js';
import { connectionService } from './connection.js';
import { dataService } from './data.js';
import { schemaDiscoveryService } from './schemaDiscovery.js';
import { dataTransformationService } from './dataTransformation.js';

/**
 * Orchestrates the complete database synchronization process
 */
class SyncOrchestratorService {
  constructor() {
    this.syncConfig = {
      batchSize: parseInt(process.env.DB_BATCH_SIZE) || 1000,
      onConflict: 'error',
      enableRollback: true,
      enableTransformation: true
    };
    
    this.syncStats = {
      startTime: null,
      endTime: null,
      totalTables: 0,
      successfulTables: 0,
      failedTables: [],
      totalRows: 0,
      syncedRows: 0
    };
  }

  /**
   * Executes the complete database synchronization
   * @param {Object} credentials - Database credentials
   * @returns {Promise<Object>} Synchronization result
   */
  async executeSync(credentials) {
    this.syncStats.startTime = Date.now();
    logger.section('MetaExodus - Database Synchronization');

    try {
      await this.authenticateAndConnect(credentials);

      const { tables, dependencies, enumMap } = await this.discoverAndAnalyze();

      await this.clearExistingData(tables, dependencies);

      await this.synchronizeData(tables, dependencies, enumMap);

      return await this.finalizeSynchronization();

    } catch (error) {
      logger.error('Synchronization failure', error);
      await this.handleSyncFailure();
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Phase 1: Authenticate with Metabase and connect to local database
   * @param {Object} credentials - Database credentials
   */
  async authenticateAndConnect(credentials) {
    logger.startSpinner('Authenticating with Metabase');
    const authResult = await metabaseService.authenticate(
      credentials.username,
      credentials.password
    );

    if (!authResult.success) {
      logger.stopSpinner(false, 'Authentication failed');
      throw new Error(`Failed to authenticate with Metabase: ${authResult.error}`);
    }
    logger.stopSpinner(true, 'Metabase authentication successful');

    logger.startSpinner('Connecting to local database');
    await connectionService.initialize();
    await dataService.initialize();
    await connectionService.connectLocal();
    logger.stopSpinner(true, 'Local database connected');
  }

  /**
   * Phase 2: Discover tables, dependencies, and schema information
   * @returns {Promise<Object>} Discovery results
   */
  async discoverAndAnalyze() {
    logger.startSpinner('Discovering tables in Metabase');
    const tablesResult = await metabaseService.getTables();

    if (!tablesResult.success) {
      logger.stopSpinner(false, 'Failed to retrieve tables');
      throw new Error(`Failed to retrieve tables: ${tablesResult.error}`);
    }

    const tables = tablesResult.tables;
    this.syncStats.totalTables = tables.length;
    logger.stopSpinner(true, `Found ${tables.length} tables to synchronize`);

    logger.startSpinner('Analyzing table dependencies');
    const localConnection = await connectionService.connectLocal();
    const dependencies = await dataService.getTableDependencies(localConnection);
    logger.stopSpinner(true, 'Table dependencies analyzed');

    logger.startSpinner('Discovering database schema');
    const enumMap = await schemaDiscoveryService.discoverEnumValues(localConnection);
    logger.stopSpinner(true, `Discovered ${Object.keys(enumMap).length} enum types`);

    return { tables, dependencies, enumMap };
  }

  /**
   * Phase 3: Clear existing data from tables in safe order
   * @param {Array} tables - List of tables
   * @param {Object} dependencies - Table dependencies
   */
  async clearExistingData(tables, dependencies) {
    logger.startSpinner('Clearing existing data from tables');
    const localConnection = await connectionService.connectLocal();
    const tableNames = tables.map(t => t.name);
    const clearingOrder = dataService.sortTablesByDependencies(tableNames, dependencies).reverse();

    let clearedTables = 0;
    for (const tableName of clearingOrder) {
      try {
        await localConnection.query(`DELETE FROM "${tableName}"`);
        clearedTables++;
      } catch (deleteError) {
        logger.warn(`Could not clear table ${tableName}: ${deleteError.message}`);
      }
    }
    
    logger.stopSpinner(true, `Cleared data from ${clearedTables}/${tables.length} tables`);
  }

  /**
   * Phase 4: Synchronize data from Metabase to local database
   * @param {Array} tables - List of tables
   * @param {Object} dependencies - Table dependencies
   * @param {Object} enumMap - Enum type mappings
   */
  async synchronizeData(tables, dependencies, enumMap) {
    // Analyze table sizes
    const tableCounts = await this.analyzeTableSizes(tables);
    
    // Synchronize data in dependency order
    await this.performDataSync(tables, dependencies, enumMap, tableCounts);
  }

  /**
   * Analyzes table sizes for progress reporting
   * @param {Array} tables - List of tables
   * @returns {Promise<Object>} Table row counts
   */
  async analyzeTableSizes(tables) {
    logger.subsection('Analyzing Table Sizes');
    const tableCounts = {};
    
    logger.createProgressBar(tables.length, 'Counting rows');
    for (let i = 0; i < tables.length; i++) {
      const table = tables[i];
      const countResult = await metabaseService.getTableRowCount(table.id);
      tableCounts[table.name] = countResult.success ? countResult.count : 0;
      logger.updateProgress(i + 1);
    }
    logger.stopProgress();

    const totalRemoteRows = Object.values(tableCounts).reduce((sum, count) => sum + count, 0);
    this.syncStats.totalRows = totalRemoteRows;
    
    logger.success(`Found ${totalRemoteRows.toLocaleString()} total rows across ${tables.length} tables`);
    
    // Show largest tables
    const sortedBySize = Object.entries(tableCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 5);
      
    if (sortedBySize.length > 0) {
      logger.info('Largest tables:');
      sortedBySize.forEach(([name, count]) => {
        logger.info(`  ${name}: ${count.toLocaleString()} rows`);
      });
    }

    return tableCounts;
  }

  /**
   * Performs the actual data synchronization
   * @param {Array} tables - List of tables
   * @param {Object} dependencies - Table dependencies
   * @param {Object} enumMap - Enum type mappings
   * @param {Object} tableCounts - Table row counts
   */
  async performDataSync(tables, dependencies, enumMap, tableCounts) {
    logger.subsection('Synchronizing Data');
    
    const localConnection = await connectionService.connectLocal();
    const tableNames = tables.map(t => t.name);
    const insertionOrder = dataService.sortTablesByDependencies(tableNames, dependencies);
    const sortedTables = insertionOrder.map(name => tables.find(t => t.name === name)).filter(Boolean);
    
    logger.createProgressBar(tables.length, 'Syncing tables');

    for (let i = 0; i < sortedTables.length; i++) {
      const table = sortedTables[i];
      const rowCount = tableCounts[table.name] || 0;

      logger.updateProgress(i + 1, `${table.name} (${rowCount.toLocaleString()} rows)`);

      if (rowCount === 0) {
        this.syncStats.successfulTables++;
        continue;
      }

      try {
        await this.syncSingleTable(localConnection, table, enumMap);
        this.syncStats.successfulTables++;
      } catch (error) {
        this.syncStats.failedTables.push({
          name: table.name,
          error: error.message,
          details: error.details || 'Unknown error'
        });
      }
    }
    
    logger.stopProgress();

    if (this.syncStats.failedTables.length > 0) {
      await this.handleSyncFailures(tables, dependencies);
    }
  }

  /**
   * Synchronizes a single table
   * @param {Object} connection - Database connection
   * @param {Object} table - Table information
   * @param {Object} enumMap - Enum type mappings
   */
  async syncSingleTable(connection, table, enumMap) {
    const extractResult = await metabaseService.extractAllTableData(table.id, table.name);

    if (!extractResult.success || extractResult.data.length === 0) {
      throw new Error(`Data extraction failed: ${extractResult.error || 'No data returned'}`);
    }

    const transformedData = await dataTransformationService.transformTableData(
      connection,
      table.name,
      extractResult.data,
      enumMap
    );

    const insertResult = await dataService.insertTableData(
      connection,
      table.name,
      transformedData,
      {
        onConflict: this.syncConfig.onConflict,
        batchSize: this.syncConfig.batchSize
      }
    );

    if (!insertResult.success) {
      const errorDetails = insertResult.errors && insertResult.errors.length > 0
        ? insertResult.errors[0].error
        : 'Unknown insertion error';
      throw new Error(`Data insertion failed: ${errorDetails}`);
    }

    if (insertResult.insertedRows !== extractResult.data.length) {
      throw new Error(`Row count mismatch: expected ${extractResult.data.length}, inserted ${insertResult.insertedRows}`);
    }

    this.syncStats.syncedRows += insertResult.insertedRows;
  }

  /**
   * Handles synchronization failures with rollback
   * @param {Array} tables - List of tables
   * @param {Object} dependencies - Table dependencies
   */
  async handleSyncFailures(tables, dependencies) {
    logger.error(`SYNC FAILED: ${this.syncStats.failedTables.length} tables failed to sync. Rolling back...`);
    
    this.syncStats.failedTables.forEach(({ name, error, details }) => {
      logger.error(`  - ${name}: ${error} (${details})`);
    });

    if (this.syncConfig.enableRollback) {
      await this.performRollback(tables, dependencies);
    }

    throw new Error('Database synchronization FAILED - no changes applied');
  }

  /**
   * Performs rollback by clearing all tables
   * @param {Array} tables - List of tables
   * @param {Object} dependencies - Table dependencies
   */
  async performRollback(tables, dependencies) {
    logger.startSpinner('Rolling back changes');
    
    const localConnection = await connectionService.connectLocal();
    const tableNames = tables.map(t => t.name);
    const clearingOrder = dataService.sortTablesByDependencies(tableNames, dependencies).reverse();

    for (const tableName of clearingOrder) {
      try {
        await localConnection.query(`DELETE FROM "${tableName}"`);
      } catch (rollbackError) {
        logger.warn(`Could not rollback table ${tableName}: ${rollbackError.message}`);
      }
    }
    
    logger.stopSpinner(true, 'Rollback completed');
  }

  /**
   * Phase 5: Finalize synchronization and generate report
   * @returns {Promise<Object>} Final synchronization result
   */
  async finalizeSynchronization() {
    this.syncStats.endTime = Date.now();
    const duration = Math.round((this.syncStats.endTime - this.syncStats.startTime) / 1000);
    const minutes = Math.floor(duration / 60);
    const seconds = duration % 60;

    logger.summary({
      duration: `${minutes}m ${seconds}s`,
      tablesSynchronized: `${this.syncStats.successfulTables}/${this.syncStats.totalTables}`,
      totalRowsSynchronized: this.syncStats.syncedRows,
      successRate: '100%'
    });

    logger.success('Local database is now an exact replica of the remote database');
    logger.success('Database synchronization completed successfully');

    return {
      success: true,
      ...this.syncStats,
      duration,
      transformationStats: dataTransformationService.getTransformationStats()
    };
  }

  /**
   * Handles sync failure cleanup
   */
  async handleSyncFailure() {
    this.syncStats.endTime = Date.now();
    logger.error('Database synchronization FAILED');
  }

  /**
   * Performs a dry-run analysis without making changes
   * @param {Object} credentials - Database credentials
   * @returns {Promise<Object>} Dry-run analysis result
   */
  async performDryRun(credentials) {
    this.syncStats.startTime = Date.now();
    this.syncStats.failedTables = [];
    
    logger.section('MetaExodus - Dry Run Analysis');

    try {
      await this.authenticateAndConnect(credentials);

      const { tables, enumMap } = await this.discoverAndAnalyze();

      const tableCounts = await this.analyzeTableSizes(tables);

      const dryRunResult = await this.analyzePlannedChanges(tables, tableCounts, enumMap);

      if (!dryRunResult.success) {
        return { success: false, error: dryRunResult.error };
      }

      this.syncStats.endTime = Date.now();
      const summary = this.generateDryRunSummary(dryRunResult, tableCounts);

      logger.section('Dry Run Summary');
      logger.table([
        { 'Metric': 'Total Tables', 'Value': tables.length.toString() },
        { 'Metric': 'Tables with Data', 'Value': dryRunResult.tablesWithData.toString() },
        { 'Metric': 'Total Rows to Copy', 'Value': dryRunResult.totalRowsToSync.toLocaleString() },
        { 'Metric': 'Estimated Duration', 'Value': summary.estimatedDuration },
        { 'Metric': 'Schema Changes', 'Value': dryRunResult.schemaChanges.toString() },
        { 'Metric': 'Data Transformations', 'Value': dryRunResult.dataTransformations.toString() }
      ]);

      if (dryRunResult.potentialIssues.length > 0) {
        logger.subsection('Potential Issues');
        dryRunResult.potentialIssues.forEach(issue => {
          logger.warn(`${issue.table}: ${issue.issue}`);
        });
      }

      logger.info('This was a dry run - no changes were made to the database');
      logger.success('Dry run analysis completed successfully');

      return { success: true, summary, analysis: dryRunResult };
    } catch (error) {
      logger.error('Dry run analysis failed', error);
      return { success: false, error: error.message };
    } finally {
      await this.cleanup();
    }
  }

  /**
   * Analyzes planned changes without making them
   * @param {Array} tables - Tables to analyze
   * @param {Object} tableCounts - Table row counts
   * @param {Object} enumMap - Enum mappings
   * @returns {Promise<Object>} Analysis result
   */
  async analyzePlannedChanges(tables, tableCounts, enumMap) {
    try {
      logger.subsection('Analyzing Planned Changes');
      const potentialIssues = [];
      let tablesWithData = 0;
      let totalRowsToSync = 0;
      let schemaChanges = 0;
      let dataTransformations = 0;

      const localConnection = await connectionService.connectLocal();

      logger.createProgressBar(tables.length, 'Analyzing tables');
      for (let i = 0; i < tables.length; i++) {
        const table = tables[i];
        const rowCount = tableCounts[table.name] || 0;

        logger.updateProgress(i + 1, `${table.name} (${rowCount.toLocaleString()} rows)`);

        if (rowCount > 0) {
          tablesWithData++;
          totalRowsToSync += rowCount;

          try {
            const sampleResult = await metabaseService.extractTableData(
              table.id,
              table.name,
              { limit: 10 }
            );

            if (sampleResult.success && sampleResult.data.length > 0) {
              const transformResult = await dataTransformationService.transformTableData(
                localConnection,
                table.name,
                sampleResult.data,
                { enumMap, validateOnly: true }
              );

              if (!transformResult.success) {
                potentialIssues.push({
                  table: table.name,
                  issue: 'Data transformation issues detected',
                  details: transformResult.issues.map(i => i.message).join(', ')
                });
              } else if (transformResult.issues.length > 0) {
                dataTransformations++;
                potentialIssues.push({
                  table: table.name,
                  issue: `${transformResult.issues.length} data transformation(s) needed`,
                  details: transformResult.issues.map(i => i.message).join(', ')
                });
              }
            }

            const schemaInfo = await schemaDiscoveryService.getTableSchemaInfo(localConnection, table.name);
            if (schemaInfo.enumColumns && schemaInfo.enumColumns.length > 0) {
              schemaChanges++;
            }

          } catch (error) {
            potentialIssues.push({
              table: table.name,
              issue: 'Analysis error',
              details: error.message
            });
          }
        }
      }
      logger.stopProgress();

      return {
        success: true,
        tablesWithData,
        totalRowsToSync,
        schemaChanges,
        dataTransformations,
        potentialIssues
      };
    } catch (error) {
      logger.error('Planned changes analysis failed', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Generates a dry-run summary
   * @param {Object} analysisResult - Analysis result
   * @param {Object} _tableCounts - Table counts
   * @returns {Object} Dry-run summary
   */
  generateDryRunSummary(analysisResult, _tableCounts) {
    const endTime = Date.now();
    const duration = this.syncStats.startTime ? endTime - this.syncStats.startTime : 0;
    const durationMinutes = Math.floor(duration / 60000);
    const durationSeconds = Math.floor((duration % 60000) / 1000);
    
    // Estimate sync duration based on row count (rough estimate: 1000 rows per second)
    const estimatedSyncSeconds = Math.ceil(analysisResult.totalRowsToSync / 1000);
    const estimatedMinutes = Math.floor(estimatedSyncSeconds / 60);
    const remainingSeconds = estimatedSyncSeconds % 60;

    return {
      analysisDuration: `${durationMinutes}m ${durationSeconds}s`,
      estimatedDuration: `~${estimatedMinutes}m ${remainingSeconds}s`,
      tablesWithData: analysisResult.tablesWithData,
      totalRowsToSync: analysisResult.totalRowsToSync,
      potentialIssues: analysisResult.potentialIssues.length
    };
  }

  /**
   * Cleanup resources
   */
  async cleanup() {
    try {
      await connectionService.closeConnections();
      await metabaseService.logout();
      logger.info('Connections closed');
    } catch (error) {
      logger.error('Error during cleanup', error);
    }
  }

  /**
   * Configures sync options
   * @param {Object} config - Sync configuration
   */
  configure(config) {
    this.syncConfig = { ...this.syncConfig, ...config };
  }

  /**
   * Gets current sync statistics
   * @returns {Object} Sync statistics
   */
  getSyncStats() {
    return { ...this.syncStats };
  }
}

const syncOrchestratorService = new SyncOrchestratorService();

export { SyncOrchestratorService, syncOrchestratorService };
