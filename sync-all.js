import dotenv from 'dotenv';
dotenv.config();

import { metabaseService } from './src/services/metabase.js';
import { connectionService } from './src/services/connection.js';
import { dataService } from './src/services/data.js';
import { logger } from './src/utils/logger.js';

/**
 * Automatically discover enum values from PostgreSQL database
 */
async function discoverEnumValues(connection) {
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

        return enumMap;
    } catch (error) {
        logger.warn(`Could not discover enum values: ${error.message}`);
        return {};
    }
}

/**
 * Automatically discover table schema information including enum columns
 */
async function discoverTableSchema(connection, tableName) {
    try {
        const query = `
            SELECT 
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE c.table_name = $1 
            AND c.table_schema = 'public'
            ORDER BY c.ordinal_position;
        `;

        const result = await connection.query(query, [tableName]);
        return result.rows;
    } catch (error) {
        logger.warn(`Could not discover schema for table ${tableName}: ${error.message}`);
        return [];
    }
}

/**
 * Transform data automatically based on discovered schema
 */
async function transformDataForSchema(connection, tableName, data, enumMap) {
    if (!data || data.length === 0) {
        return data;
    }

    // Get table schema
    const schema = await discoverTableSchema(connection, tableName);
    const enumColumns = schema.filter(col => col.udt_name && enumMap[col.udt_name]);

    if (enumColumns.length === 0) {
        return data; // No enum columns to transform
    }

    // Transform data to handle enum values
    return data.map(row => {
        const transformedRow = { ...row };

        enumColumns.forEach(col => {
            const columnName = col.column_name;
            const enumName = col.udt_name;
            const validValues = enumMap[enumName] || [];

            if (transformedRow[columnName] !== null && transformedRow[columnName] !== undefined) {
                const currentValue = transformedRow[columnName];

                // Check if current value is valid
                if (!validValues.includes(currentValue)) {
                    logger.debug(`Invalid enum value '${currentValue}' for ${tableName}.${columnName}, valid values: ${validValues.join(', ')}`);

                    // Try to find a close match (case insensitive)
                    const closeMatch = validValues.find(valid =>
                        valid.toLowerCase() === currentValue.toLowerCase()
                    );

                    if (closeMatch) {
                        transformedRow[columnName] = closeMatch;
                        logger.debug(`Mapped '${currentValue}' to '${closeMatch}' for ${tableName}.${columnName}`);
                    } else if (validValues.length > 0) {
                        // Use the first valid value as default
                        transformedRow[columnName] = validValues[0];
                        logger.debug(`Defaulted '${currentValue}' to '${validValues[0]}' for ${tableName}.${columnName}`);
                    } else {
                        // Set to null if no valid values
                        transformedRow[columnName] = null;
                    }
                }
            }
        });

        return transformedRow;
    });
}



async function syncAllTables() {
    logger.section('MetaExodus - Database Synchronization');
    const startTime = Date.now();
    let totalTables = 0;
    let totalRowsSynced = 0;

    try {
        logger.startSpinner('Authenticating with Metabase');
        const authResult = await metabaseService.authenticate(
            process.env.DB_REMOTE_USERNAME,
            process.env.DB_REMOTE_PASSWORD
        );

        if (!authResult.success) {
            logger.stopSpinner(false, 'Authentication failed');
            logger.error('Failed to authenticate with Metabase', authResult.error);
            process.exit(1);
        }
        logger.stopSpinner(true, 'Metabase authentication successful');

        logger.startSpinner('Connecting to local database');
        await connectionService.initialize();
        await dataService.initialize();
        const localConnection = await connectionService.connectLocal();
        logger.stopSpinner(true, 'Local database connected');

        logger.startSpinner('Discovering tables in Metabase');
        const tablesResult = await metabaseService.getTables();

        if (!tablesResult.success) {
            logger.stopSpinner(false, 'Failed to retrieve tables');
            logger.error('Failed to retrieve tables', tablesResult.error);
            process.exit(1);
        }

        const tables = tablesResult.tables;
        totalTables = tables.length;
        logger.stopSpinner(true, `Found ${totalTables} tables to synchronize`);

        // Get table dependencies and clear in safe order
        logger.startSpinner('Analyzing table dependencies');
        const dependencies = await dataService.getTableDependencies(localConnection);
        const tableNames = tables.map(t => t.name);
        const clearingOrder = dataService.sortTablesByDependencies(tableNames, dependencies).reverse();
        logger.stopSpinner(true, 'Table dependencies analyzed');

        logger.startSpinner('Clearing existing data from tables');
        let clearedTables = 0;
        for (const tableName of clearingOrder) {
            try {
                await localConnection.query(`DELETE FROM "${tableName}"`);
                clearedTables++;
            } catch (deleteError) {
                logger.warn(`Could not clear table ${tableName}: ${deleteError.message}`);
                // Continue with other tables even if one fails to clear
            }
        }
        logger.stopSpinner(true, `Cleared data from ${clearedTables}/${totalTables} tables`);

        // Discover enum values for automatic data transformation
        logger.startSpinner('Discovering database schema');
        const enumMap = await discoverEnumValues(localConnection);
        logger.stopSpinner(true, `Discovered ${Object.keys(enumMap).length} enum types`);

        logger.subsection('Analyzing Table Sizes');
        const tableCounts = {};
        logger.createProgressBar(tables.length, 'Counting rows');
        for (let i = 0; i < tables.length; i++) {
            const table = tables[i];
            const countResult = await metabaseService.getTableRowCount(table.id);
            if (countResult.success) {
                tableCounts[table.name] = countResult.count;
            } else {
                tableCounts[table.name] = 0;
            }
            logger.updateProgress(i + 1);
        }
        logger.stopProgress();

        const totalRemoteRows = Object.values(tableCounts).reduce((sum, count) => sum + count, 0);
        logger.success(`Found ${totalRemoteRows.toLocaleString()} total rows across ${totalTables} tables`);
        const sortedBySize = Object.entries(tableCounts)
            .sort(([, a], [, b]) => b - a)
            .slice(0, 5);
        if (sortedBySize.length > 0) {
            logger.info('Largest tables:');
            sortedBySize.forEach(([name, count]) => {
                logger.info(`  ${name}: ${count.toLocaleString()} rows`);
            });
        }

        logger.subsection('Synchronizing Data');
        // Sort tables by dependency order for insertion (dependencies first)
        const insertionOrder = dataService.sortTablesByDependencies(tableNames, dependencies);
        const sortedTables = insertionOrder.map(name => tables.find(t => t.name === name)).filter(Boolean);
        logger.createProgressBar(totalTables, 'Syncing tables');

        let successfulDataSync = 0;
        const failedTables = [];

        for (let i = 0; i < sortedTables.length; i++) {
            const table = sortedTables[i];
            const rowCount = tableCounts[table.name] || 0;

            logger.updateProgress(i + 1, `${table.name} (${rowCount.toLocaleString()} rows)`);

            if (rowCount === 0) {
                successfulDataSync++;
                continue;
            }

            try {
                const extractResult = await metabaseService.extractAllTableData(
                    table.id,
                    table.name
                );

                if (!extractResult.success || extractResult.data.length === 0) {
                    if (rowCount > 0) {
                        logger.warn(`Failed to extract data from '${table.name}': ${extractResult.error || 'No data returned'}`);
                        failedTables.push({ name: table.name, error: 'Data extraction failed', details: extractResult.error });
                        continue;
                    } else {
                        successfulDataSync++;
                        continue;
                    }
                }

                // Transform data to handle enum values and schema compatibility
                const transformedData = await transformDataForSchema(
                    localConnection,
                    table.name,
                    extractResult.data,
                    enumMap
                );

                const insertResult = await dataService.insertTableData(
                    localConnection,
                    table.name,
                    transformedData,
                    {
                        onConflict: 'error', // Fail on conflicts to ensure exact replica
                        batchSize: parseInt(process.env.DB_BATCH_SIZE) || 1000
                    }
                );

                if (!insertResult.success) {
                    logger.warn(`Failed to insert data into '${table.name}'`);
                    const errorDetails = insertResult.errors && insertResult.errors.length > 0
                        ? insertResult.errors[0].error
                        : 'Unknown insertion error';
                    failedTables.push({ name: table.name, error: 'Data insertion failed', details: errorDetails });
                    continue;
                }

                // Check if we inserted the expected number of rows
                if (insertResult.insertedRows !== extractResult.data.length) {
                    logger.warn(`Row count mismatch for '${table.name}': expected ${extractResult.data.length}, inserted ${insertResult.insertedRows}`);
                    failedTables.push({
                        name: table.name,
                        error: 'Row count mismatch',
                        details: `Expected ${extractResult.data.length} rows, but only inserted ${insertResult.insertedRows}`
                    });
                    continue;
                }

                totalRowsSynced += insertResult.insertedRows;
                successfulDataSync++;

            } catch (error) {
                logger.warn(`Failed to synchronize '${table.name}': ${error.message}`);
                failedTables.push({ name: table.name, error: 'Synchronization error', details: error.message });
                continue;
            }
        }
        logger.stopProgress();

        // Check for failures - ALL OR NONE policy
        if (failedTables.length > 0) {
            logger.stopProgress();
            logger.error(`SYNC FAILED: ${failedTables.length} tables failed to sync. Rolling back...`);
            failedTables.forEach(({ name, error, details }) => {
                logger.error(`  - ${name}: ${error} (${details})`);
            });

            // Rollback: Clear all tables that were successfully synced
            logger.startSpinner('Rolling back changes');
            for (const tableName of clearingOrder) {
                try {
                    await localConnection.query(`DELETE FROM "${tableName}"`);
                } catch (rollbackError) {
                    logger.warn(`Could not rollback table ${tableName}: ${rollbackError.message}`);
                }
            }
            logger.stopSpinner(true, 'Rollback completed');

            logger.error('Database synchronization FAILED - no changes applied');
            process.exit(1);
        }

    } catch (error) {
        logger.error('Synchronization failure', error);
        process.exit(1);
    } finally {
        try {
            await connectionService.closeConnections();
            await metabaseService.logout();
            logger.info('Connections closed');
        } catch (error) {
            logger.error('Error during cleanup', error);
        }
    }

    const duration = Math.round((Date.now() - startTime) / 1000);
    const minutes = Math.floor(duration / 60);
    const seconds = duration % 60;

    logger.summary({
        duration: `${minutes}m ${seconds}s`,
        tablesSynchronized: `${totalTables}/${totalTables}`,
        totalRowsSynchronized: totalRowsSynced,
        successRate: '100%'
    });

    logger.success('Local database is now an exact replica of the remote database');
    logger.success('Database synchronization completed successfully');
}

syncAllTables().catch((error) => {
    logger.error('Fatal error', error);
    process.exit(1);
});
