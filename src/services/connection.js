import { Pool, Client } from 'pg';
import { configManager } from '../config/database.js';
import { logger } from '../utils/logger.js';

class ConnectionService {
  constructor() {
    this.localPool = null;
    this.remotePool = null;
    this.localClient = null;
    this.remoteClient = null;
    this.isInitialized = false;
    this.retryConfig = {
      maxRetries: 3,
      baseDelay: 1000,
      maxDelay: 10000,
      backoffFactor: 2
    };
    this.connectionAttempts = new Map();
  }

  async initialize() {
    try {
      if (!configManager.isReady()) {
        const initResult = configManager.initialize();
        if (!initResult.success) {
          return {
            success: false,
            error: 'Failed to initialize database configurations',
            details: initResult.details
          };
        }
      }

      this.isInitialized = true;
      return {
        success: true,
        message: 'Connection service initialized successfully'
      };
    } catch (error) {
      return {
        success: false,
        error: 'Failed to initialize connection service',
        details: [error.message]
      };
    }
  }

  async connectLocal() {
    if (!this.isInitialized) {
      throw new Error('Connection service not initialized. Call initialize() first.');
    }

    return this.connectWithRetry('local', async () => {
      const localConfig = configManager.getLocalConfig();
      const connectionOptions = localConfig.getConnectionOptions();

      try {
        this.localClient = new Client(connectionOptions);
        await this.localClient.connect();
        return this.localClient;
      } catch (error) {
        // If the database does not exist, attempt to create it then reconnect
        const databaseDoesNotExist = error && (error.code === '3D000' || /database .* does not exist/i.test(error.message));
        if (databaseDoesNotExist) {
          logger.warn(`Local database "${localConfig.database}" not found. Attempting to create it...`);
          await this.createDatabaseIfNotExists();
          // Retry connect after creating database
          this.localClient = new Client(connectionOptions);
          await this.localClient.connect();
          return this.localClient;
        }
        throw error;
      }
    });
  }

  async connectWithRetry(connectionType, connectFunction) {
    const attempts = this.connectionAttempts.get(connectionType) || 0;

    try {
      const result = await connectFunction();
      this.connectionAttempts.set(connectionType, 0);
      return result;
    } catch (error) {
      const newAttempts = attempts + 1;
      this.connectionAttempts.set(connectionType, newAttempts);

      if (newAttempts >= this.retryConfig.maxRetries) {
        this.connectionAttempts.set(connectionType, 0);
        throw new Error(`Failed to connect to ${connectionType} database after ${this.retryConfig.maxRetries} attempts: ${error.message}`);
      }

      const delay = Math.min(
        this.retryConfig.baseDelay * Math.pow(this.retryConfig.backoffFactor, attempts),
        this.retryConfig.maxDelay
      );

      logger.warn(`Connection attempt ${newAttempts} failed for ${connectionType} database. Retrying in ${delay}ms`);

      await this.sleep(delay);

      return this.connectWithRetry(connectionType, connectFunction);
    }
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async createLocalPool() {
    if (!this.isInitialized) {
      throw new Error('Connection service not initialized. Call initialize() first.');
    }

    try {
      const localConfig = configManager.getLocalConfig();
      const connectionOptions = localConfig.getConnectionOptions();

      this.localPool = new Pool(connectionOptions);

      const client = await this.localPool.connect();
      client.release();

      return this.localPool;
    } catch (error) {
      throw new Error(`Failed to create local database pool: ${error.message}`);
    }
  }

  async createDatabaseIfNotExists() {
    const localConfig = configManager.getLocalConfig();
    const adminConnectionOptions = {
      ...localConfig.getConnectionOptions(),
      database: 'postgres'
    };

    const adminClient = new Client(adminConnectionOptions);
    try {
      await adminClient.connect();
      const dbName = localConfig.database;
      const owner = localConfig.username;
      // Ensure identifier quoting to handle special names
      const quotedDbName = '"' + dbName.replace(/"/g, '""') + '"';
      const quotedOwner = '"' + owner.replace(/"/g, '""') + '"';

      // Check existence
      const existsResult = await adminClient.query(
        'SELECT 1 FROM pg_database WHERE datname = $1',
        [dbName]
      );
      if (existsResult.rowCount > 0) {
        logger.info(`Database ${dbName} already exists`);
        return;
      }

      // Create database with UTF8 encoding
      const createSql = `CREATE DATABASE ${quotedDbName} OWNER ${quotedOwner} TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'`;
      await adminClient.query(createSql);
      logger.success ? logger.success(`Created database ${dbName}`) : logger.info(`Created database ${dbName}`);
    } catch (err) {
      throw new Error(`Failed to create database: ${err.message}`);
    } finally {
      try { await adminClient.end(); } catch {}
    }
  }

  async testConnections() {
    const results = {
      local: { success: false, error: null },
      overall: false
    };

    try {
      const localClient = await this.connectLocal();
      await localClient.query('SELECT 1');
      await localClient.end();
      results.local.success = true;
    } catch (error) {
      results.local.error = error.message;
    }

    results.overall = results.local.success;
    return results;
  }

  async checkConnectionHealth(connection) {
    try {
      const startTime = Date.now();

      if (connection.connect && typeof connection.connect === 'function' && !connection.query) {
        const client = await connection.connect();
        await client.query('SELECT 1 as health_check');
        client.release();
      } else {
        await connection.query('SELECT 1 as health_check');
      }

      const responseTime = Date.now() - startTime;

      return {
        healthy: true,
        responseTime,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  getConnectionInfo() {
    if (!this.isInitialized) {
      return { initialized: false };
    }

    const maskedConfigs = configManager.getMaskedConfigs();

    return {
      initialized: this.isInitialized,
      localPool: this.localPool ? {
        totalCount: this.localPool.totalCount,
        idleCount: this.localPool.idleCount,
        waitingCount: this.localPool.waitingCount
      } : null,
      remotePool: this.remotePool ? {
        totalCount: this.remotePool.totalCount,
        idleCount: this.remotePool.idleCount,
        waitingCount: this.remotePool.waitingCount
      } : null,
      retryAttempts: Object.fromEntries(this.connectionAttempts),
      configurations: maskedConfigs
    };
  }

  resetRetryAttempts(connectionType) {
    this.connectionAttempts.set(connectionType, 0);
  }

  configureRetry(retryConfig) {
    this.retryConfig = { ...this.retryConfig, ...retryConfig };
  }

  getRetryConfig() {
    return { ...this.retryConfig };
  }

  async monitorPools() {
    const results = {
      local: null,
      remote: null,
      timestamp: new Date().toISOString()
    };

    if (this.localPool) {
      try {
        const health = await this.checkConnectionHealth(this.localPool);
        results.local = {
          ...health,
          poolStats: health.healthy ? {
            totalCount: this.localPool.totalCount,
            idleCount: this.localPool.idleCount,
            waitingCount: this.localPool.waitingCount
          } : null
        };
      } catch (error) {
        results.local = {
          healthy: false,
          error: error.message,
          poolStats: null
        };
      }
    }

    if (this.remotePool) {
      try {
        const health = await this.checkConnectionHealth(this.remotePool);
        results.remote = {
          ...health,
          poolStats: health.healthy ? {
            totalCount: this.remotePool.totalCount,
            idleCount: this.remotePool.idleCount,
            waitingCount: this.remotePool.waitingCount
          } : null
        };
      } catch (error) {
        results.remote = {
          healthy: false,
          error: error.message,
          poolStats: null
        };
      }
    }

    return results;
  }

  async closeConnections() {
    const errors = [];

    try {
      if (this.localClient) {
        await this.localClient.end();
        this.localClient = null;
      }
    } catch (error) {
      errors.push(`Error closing local client: ${error.message}`);
    }

    try {
      if (this.remoteClient) {
        await this.remoteClient.end();
        this.remoteClient = null;
      }
    } catch (error) {
      errors.push(`Error closing remote client: ${error.message}`);
    }

    try {
      if (this.localPool) {
        await this.localPool.end();
        this.localPool = null;
      }
    } catch (error) {
      errors.push(`Error closing local pool: ${error.message}`);
    }

    try {
      if (this.remotePool) {
        await this.remotePool.end();
        this.remotePool = null;
      }
    } catch (error) {
      errors.push(`Error closing remote pool: ${error.message}`);
    }

    if (errors.length > 0) {
      throw new Error(`Errors during cleanup: ${errors.join(', ')}`);
    }
  }
}

const connectionService = new ConnectionService();

export { ConnectionService, connectionService };
