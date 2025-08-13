#!/usr/bin/env node

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';

class DataDebugger {
  constructor() {
    this.baseURL = process.env.METABASE_BASE_URL;
    this.databaseId = process.env.METABASE_DATABASE_ID;
    this.username = process.env.DB_REMOTE_USERNAME;
    this.password = process.env.DB_REMOTE_PASSWORD;
    this.sessionToken = null;
  }

  async authenticate() {
    try {
      console.log('🔐 Authenticating with Metabase...');
      const response = await axios.post(`${this.baseURL}/api/session`, {
        username: this.username,
        password: this.password
      });
      
      this.sessionToken = response.data.id;
      console.log('✅ Authentication successful');
      return true;
    } catch (error) {
      console.error('❌ Authentication failed:', error.response?.data || error.message);
      return false;
    }
  }

  async getTableData(tableId, tableName, limit = 5) {
    try {
      const query = {
        database: this.databaseId,
        type: 'query',
        query: {
          'source-table': tableId,
          limit: limit
        }
      };

      const response = await axios.post(`${this.baseURL}/api/dataset`, query, {
        headers: {
          'X-Metabase-Session': this.sessionToken,
          'Content-Type': 'application/json'
        }
      });

      const data = response.data;
      const columns = data.data.cols.map(col => col.name);
      const rows = data.data.rows.map(row => {
        const rowObj = {};
        columns.forEach((col, index) => {
          rowObj[col] = row[index];
        });
        return rowObj;
      });

      return { columns, rows };
    } catch (error) {
      console.error(`❌ Failed to get data for ${tableName}:`, error.response?.data || error.message);
      return null;
    }
  }

  async debugTable(tableId, tableName) {
    console.log(`\n🔍 Debugging table: ${tableName}`);
    console.log('=' .repeat(50));
    
    const data = await this.getTableData(tableId, tableName, 3);
    if (!data) return;

    console.log(`Columns: ${data.columns.join(', ')}`);
    console.log('\nSample data:');
    data.rows.forEach((row, index) => {
      console.log(`\nRow ${index + 1}:`);
      Object.entries(row).forEach(([key, value]) => {
        const displayValue = typeof value === 'string' && value.length > 100 
          ? value.substring(0, 100) + '...' 
          : value;
        console.log(`  ${key}: ${displayValue}`);
      });
    });
  }

  async debug() {
    if (!await this.authenticate()) {
      return;
    }

    // Debug the problematic tables
    await this.debugTable(1, 'admin_logs'); // You'll need to find the correct table ID
    await this.debugTable(2, 'user'); // You'll need to find the correct table ID
  }
}

async function main() {
  console.log('🐛 MetaExodus Data Debugger');
  console.log('============================\n');

  const debugger = new DataDebugger();
  await debugger.debug();
}

main();
