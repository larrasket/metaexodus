import { logger } from '../utils/logger.js';

class SchemaCreationService {
  constructor() {
    this.typeMap = {
      'type/Text': 'TEXT',
      'type/Integer': 'INTEGER',
      'type/Float': 'DOUBLE PRECISION',
      'type/Boolean': 'BOOLEAN',
      'type/Date': 'DATE',
      'type/DateTime': 'TIMESTAMP',
      'type/Time': 'TIME',
      'type/Decimal': 'DECIMAL',
      'type/BigInteger': 'BIGINT',
      'type/SerializedJSON': 'JSONB',
      // Metabase often sends arrays as JSON strings; store as JSONB for compatibility
      'type/Array': 'JSONB',
      'type/UUID': 'UUID'
    };
  }

  mapMetabaseTypeToPostgres(metabaseType) {
    return this.typeMap[metabaseType] || 'TEXT';
  }

  async ensureSchema(connection, tables) {
    if (!tables || tables.length === 0) {
      logger.info('No tables found in Metabase metadata');
      return { created: 0, altered: 0 };
    }

    let created = 0;
    let altered = 0;

    for (const table of tables) {
      const tableName = table.name;
      const exists = await this.tableExists(connection, tableName);
      if (!exists) {
        const ok = await this.createTable(connection, table);
        if (ok) {
          created++;
        }
        continue;
      }

      // Align existing columns (upgrade to JSONB when needed) and add missing ones
      const changed = await this.alignAndExtendColumns(connection, table);
      if (changed > 0) {
        altered += changed;
      }
    }

    if (created > 0 || altered > 0) {
      if (logger.success) {
        logger.success(
          `Schema ensured. Created ${created} tables, added ${altered} columns`
        );
      } else {
        logger.info(
          `Schema ensured. Created ${created} tables, added ${altered} columns`
        );
      }
    } else {
      logger.info('Schema is up to date');
    }

    return { created, altered };
  }

  async getColumnDefinitions(connection, tableName) {
    const result = await connection.query(
      `SELECT column_name, data_type, udt_name
       FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = $1`,
      [tableName]
    );
    const map = new Map();
    for (const row of result.rows) {
      map.set(row.column_name, {
        data_type: row.data_type,
        udt_name: row.udt_name
      });
    }
    return map;
  }

  async alignAndExtendColumns(connection, table) {
    let changed = 0;
    const existingCols = await this.getExistingColumns(connection, table.name);
    const existingDefs = await this.getColumnDefinitions(
      connection,
      table.name
    );

    // Upgrade existing columns to JSONB when Metabase marks them as arrays/JSON
    for (const field of table.fields || []) {
      const targetType = this.mapMetabaseTypeToPostgres(field.base_type);
      if (!existingCols.has(String(field.name))) {
        continue;
      }
      if (targetType === 'JSONB') {
        const info = existingDefs.get(String(field.name));
        const isJson = info?.data_type?.toLowerCase().includes('json');
        if (!isJson) {
          const colName = `"${String(field.name).replace(/"/g, '""')}"`;
          const tableName = `"${String(table.name).replace(/"/g, '""')}"`;
          const sql = `ALTER TABLE ${tableName} ALTER COLUMN ${colName} TYPE JSONB USING to_jsonb(${colName});`;
          try {
            await connection.query(sql);
            changed++;
            logger.info(`Altered ${table.name}.${field.name} to JSONB`);
          } catch (e) {
            logger.warn(
              `Could not alter ${table.name}.${field.name} to JSONB: ${e.message}`
            );
          }
        }
      }
    }

    // Add missing columns
    changed += await this.addMissingColumns(connection, table);
    return changed;
  }

  async tableExists(connection, tableName) {
    try {
      const result = await connection.query(
        `SELECT EXISTS (
           SELECT 1 FROM information_schema.tables 
           WHERE table_schema = 'public' AND table_name = $1
         ) AS exists;`,
        [tableName]
      );
      const rows = result && Array.isArray(result.rows) ? result.rows : [];
      return Boolean(rows[0]?.exists);
    } catch (err) {
      logger.warn(
        `Could not check existence for table ${tableName}: ${err.message}`
      );
      return false;
    }
  }

  async getExistingColumns(connection, tableName) {
    try {
      const result = await connection.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`,
        [tableName]
      );
      const rows = result && Array.isArray(result.rows) ? result.rows : [];
      return new Set(rows.map((r) => r.column_name));
    } catch (err) {
      logger.warn(`Could not fetch columns for ${tableName}: ${err.message}`);
      return new Set();
    }
  }

  async createTable(connection, table) {
    try {
      const columnsSql = (table.fields || [])
        .map((field) => {
          const colName = `"${String(field.name).replace(/"/g, '""')}"`;
          const type = this.mapMetabaseTypeToPostgres(field.base_type);
          const def = `${colName} ${type}`;
          return field.semantic_type === 'type/PK' ? `${def} PRIMARY KEY` : def;
        })
        .join(', ');

      if (!columnsSql) {
        logger.warn(
          `No columns found for table ${table.name}; skipping creation`
        );
        return false;
      }

      const tableName = `"${String(table.name).replace(/"/g, '""')}"`;
      const sql = `CREATE TABLE IF NOT EXISTS ${tableName} (${columnsSql});`;
      await connection.query(sql);
      logger.info(`Created table ${table.name}`);
      return true;
    } catch (err) {
      logger.error(`Failed to create table ${table.name}`, err);
      return false;
    }
  }

  async addMissingColumns(connection, table) {
    try {
      const existing = await this.getExistingColumns(connection, table.name);
      const missingFields = (table.fields || []).filter(
        (f) => !existing.has(String(f.name))
      );
      let added = 0;
      for (const field of missingFields) {
        const colName = `"${String(field.name).replace(/"/g, '""')}"`;
        const type = this.mapMetabaseTypeToPostgres(field.base_type);
        const tableName = `"${String(table.name).replace(/"/g, '""')}"`;
        const sql = `ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS ${colName} ${type};`;
        await connection.query(sql);
        added++;
      }
      if (added > 0) {
        logger.info(`Added ${added} missing column(s) to ${table.name}`);
      }
      return added;
    } catch (err) {
      logger.error(`Failed to add missing columns for ${table.name}`, err);
      return 0;
    }
  }
}

const schemaCreationService = new SchemaCreationService();

export { SchemaCreationService, schemaCreationService };
