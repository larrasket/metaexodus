#!/usr/bin/env node

import dotenv from "dotenv";
dotenv.config();

import { Pool } from "pg";
import axios from "axios";
import { logger } from "../src/utils/logger.js";

// Tables to exclude due to data complexity issues
const EXCLUDED_TABLES = ["admin_logs", "user"];

class PartialSyncService {
  constructor() {
    this.baseURL = process.env.METABASE_BASE_URL;
    this.databaseId = process.env.METABASE_DATABASE_ID;
    this.username = process.env.DB_REMOTE_USERNAME;
    this.password = process.env.DB_REMOTE_PASSWORD;
    this.sessionToken = null;

    this.localPool = new Pool({
      host: process.env.DB_LOCAL_HOST,
      port: process.env.DB_LOCAL_PORT,
      database: process.env.DB_LOCAL_NAME,
      user: process.env.DB_LOCAL_USERNAME,
      password: process.env.DB_LOCAL_PASSWORD,
      ssl: process.env.DB_LOCAL_SSL === "true",
    });
  }

  async authenticate() {
    try {
      console.log("🔐 Authenticating with Metabase...");
      const response = await axios.post(`${this.baseURL}/api/session`, {
        username: this.username,
        password: this.password,
      });

      this.sessionToken = response.data.id;
      console.log("✅ Authentication successful");
      return true;
    } catch (error) {
      console.error(
        "❌ Authentication failed:",
        error.response?.data || error.message
      );
      return false;
    }
  }

  async getTables() {
    try {
      console.log("📋 Fetching tables from Metabase...");
      const response = await axios.get(
        `${this.baseURL}/api/database/${this.databaseId}/metadata`,
        {
          headers: {
            "X-Metabase-Session": this.sessionToken,
          },
        }
      );

      const allTables = response.data.tables || [];
      const filteredTables = allTables.filter(
        (table) => !EXCLUDED_TABLES.includes(table.name)
      );

      console.log(
        `✅ Found ${allTables.length} total tables, syncing ${filteredTables.length} (excluding ${EXCLUDED_TABLES.length} problematic tables)`
      );
      return filteredTables;
    } catch (error) {
      console.error(
        "❌ Failed to fetch tables:",
        error.response?.data || error.message
      );
      return [];
    }
  }

  async clearTableData(tableName) {
    try {
      await this.localPool.query(`DELETE FROM "${tableName}"`);
      console.log(`🗑️  Cleared data from ${tableName}`);
      return true;
    } catch (error) {
      console.warn(`⚠️  Could not clear table ${tableName}: ${error.message}`);
      return false;
    }
  }

  async getTableData(tableId, tableName) {
    try {
      const query = {
        database: this.databaseId,
        type: "query",
        query: {
          "source-table": tableId,
        },
      };

      const response = await axios.post(`${this.baseURL}/api/dataset`, query, {
        headers: {
          "X-Metabase-Session": this.sessionToken,
          "Content-Type": "application/json",
        },
      });

      const data = response.data;
      const columns = data.data.cols.map((col) => col.name);
      const rows = data.data.rows.map((row) => {
        const rowObj = {};
        columns.forEach((col, index) => {
          rowObj[col] = row[index];
        });
        return rowObj;
      });

      return { columns, rows };
    } catch (error) {
      console.error(
        `❌ Failed to get data for ${tableName}:`,
        error.response?.data || error.message
      );
      return null;
    }
  }

  async insertTableData(tableName, data) {
    if (!data || !data.rows || data.rows.length === 0) {
      console.log(`ℹ️  No data to insert for ${tableName}`);
      return true;
    }

    try {
      const columns = data.columns;
      const rows = data.rows;

      if (rows.length === 0) {
        console.log(`ℹ️  No rows to insert for ${tableName}`);
        return true;
      }

      // Create the INSERT statement
      const columnNames = columns.map((col) => `"${col}"`).join(", ");
      const placeholders = columns
        .map((_, index) => `$${index + 1}`)
        .join(", ");

      const insertSQL = `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`;

      // Insert data in batches
      const batchSize = 100;
      let insertedRows = 0;

      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);

        for (const row of batch) {
          const values = columns.map((col) => row[col]);
          await this.localPool.query(insertSQL, values);
          insertedRows++;
        }
      }

      console.log(`✅ Inserted ${insertedRows} rows into ${tableName}`);
      return true;
    } catch (error) {
      console.error(
        `❌ Failed to insert data into ${tableName}:`,
        error.message
      );
      return false;
    }
  }

  async syncTable(table) {
    console.log(`\n🔄 Syncing table: ${table.name}`);

    // Clear existing data
    await this.clearTableData(table.name);

    // Get data from Metabase
    const data = await this.getTableData(table.id, table.name);
    if (!data) {
      console.log(`⚠️  Skipping ${table.name} - no data retrieved`);
      return false;
    }

    // Insert data into local database
    const success = await this.insertTableData(table.name, data);
    return success;
  }

  async syncAll() {
    console.log("🚀 MetaExodus - Partial Sync (Excluding Problematic Tables)");
    console.log(
      "============================================================\n"
    );

    console.log(`⚠️  Excluding ${EXCLUDED_TABLES.length} problematic tables:`);
    EXCLUDED_TABLES.forEach((table) => console.log(`   - ${table}`));
    console.log("");

    if (!(await this.authenticate())) {
      return false;
    }

    const tables = await this.getTables();
    if (tables.length === 0) {
      console.log("❌ No tables to sync");
      return false;
    }

    console.log("\n🔄 Starting synchronization...\n");

    let successCount = 0;
    let failCount = 0;

    for (const table of tables) {
      if (await this.syncTable(table)) {
        successCount++;
      } else {
        failCount++;
      }
    }

    console.log("\n📈 Sync Summary:");
    console.log(`✅ Successfully synced: ${successCount} tables`);
    console.log(`❌ Failed to sync: ${failCount} tables`);
    console.log(`⚠️  Excluded: ${EXCLUDED_TABLES.length} problematic tables`);

    return successCount > 0;
  }

  async cleanup() {
    await this.localPool.end();
  }
}

async function main() {
  const syncService = new PartialSyncService();

  try {
    const success = await syncService.syncAll();

    if (success) {
      console.log("\n🎉 Partial synchronization completed successfully!");
      console.log(
        "💡 You can manually handle the excluded tables later if needed."
      );
    } else {
      console.log("\n❌ Synchronization failed");
      process.exit(1);
    }
  } catch (error) {
    console.error("💥 Unexpected error:", error);
    process.exit(1);
  } finally {
    await syncService.cleanup();
  }
}

main();
