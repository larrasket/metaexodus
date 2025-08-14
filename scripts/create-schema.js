#!/usr/bin/env node

import dotenv from 'dotenv';

dotenv.config();

import axios from 'axios';
import { Pool } from 'pg';

class SchemaCreator {
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
      ssl: process.env.DB_LOCAL_SSL === 'true'
    });
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
      console.error(
        '❌ Authentication failed:',
        error.response?.data || error.message
      );
      return false;
    }
  }

  async getTables() {
    try {
      console.log('📋 Fetching table schema from Metabase...');
      const response = await axios.get(
        `${this.baseURL}/api/database/${this.databaseId}/metadata`,
        {
          headers: {
            'X-Metabase-Session': this.sessionToken
          }
        }
      );

      return response.data.tables || [];
    } catch (error) {
      console.error(
        '❌ Failed to fetch tables:',
        error.response?.data || error.message
      );
      return [];
    }
  }

  async createTable(table) {
    try {
      const columns = table.fields
        ?.map((field) => {
          // Quote column names to handle reserved words
          const quotedColumnName = `"${field.name}"`;
          const columnDef = `${quotedColumnName} ${this.mapMetabaseTypeToPostgres(
            field.base_type
          )}`;
          return field.semantic_type === 'type/PK'
            ? `${columnDef} PRIMARY KEY`
            : columnDef;
        })
        .join(', ');

      if (!columns) {
        console.warn(`⚠️  No columns found for table ${table.name}`);
        return false;
      }

      const createTableSQL = `
        CREATE TABLE IF NOT EXISTS "${table.name}" (
          ${columns}
        );
      `;

      await this.localPool.query(createTableSQL);
      console.log(`✅ Created table: ${table.name}`);
      return true;
    } catch (error) {
      console.error(`❌ Failed to create table ${table.name}:`, error.message);
      return false;
    }
  }

  mapMetabaseTypeToPostgres(metabaseType) {
    const typeMap = {
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
      'type/Array': 'TEXT[]',
      'type/UUID': 'UUID'
    };

    return typeMap[metabaseType] || 'TEXT';
  }

  async createSchema() {
    if (!(await this.authenticate())) {
      return false;
    }

    const tables = await this.getTables();
    console.log(`📊 Found ${tables.length} tables to create`);

    let successCount = 0;
    let failCount = 0;

    for (const table of tables) {
      if (await this.createTable(table)) {
        successCount++;
      } else {
        failCount++;
      }
    }

    console.log(`\n📈 Schema Creation Summary:`);
    console.log(`✅ Successfully created: ${successCount} tables`);
    console.log(`❌ Failed to create: ${failCount} tables`);

    return successCount > 0;
  }

  async cleanup() {
    await this.localPool.end();
  }
}

async function main() {
  console.log('🏗️  MetaExodus Schema Creator');
  console.log('================================\n');

  const creator = new SchemaCreator();

  try {
    const success = await creator.createSchema();

    if (success) {
      console.log('\n🎉 Schema creation completed!');
      console.log('You can now run the full sync with: yarn start');
    } else {
      console.log('\n❌ Schema creation failed');
      process.exit(1);
    }
  } catch (error) {
    console.error('💥 Unexpected error:', error);
    process.exit(1);
  } finally {
    await creator.cleanup();
  }
}

main();
