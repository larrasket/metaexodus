import dotenv from 'dotenv';
dotenv.config();

import { syncOrchestratorService } from './src/services/syncOrchestrator.js';
import { logger } from './src/utils/logger.js';

/**
 * Main synchronization entry point
 */
async function main() {
  try {
    const credentials = {
      username: process.env.DB_REMOTE_USERNAME,
      password: process.env.DB_REMOTE_PASSWORD
    };

    if (!credentials.username || !credentials.password) {
      throw new Error('Missing required environment variables: DB_REMOTE_USERNAME, DB_REMOTE_PASSWORD');
    }

    await syncOrchestratorService.executeSync(credentials);
  } catch (error) {
    logger.error('Fatal error', error);
    process.exit(1);
  }
}

main();
