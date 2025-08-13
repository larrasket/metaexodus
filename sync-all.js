#!/usr/bin/env node

import dotenv from 'dotenv';

dotenv.config();

import { syncOrchestratorService } from './src/services/syncOrchestrator.js';
import { logger } from './src/utils/logger.js';

/**
 * Displays usage information
 */
function showUsage() {
  console.log(`
MetaExodus - Database Synchronization Tool

Usage:
  node sync-all.js [options]

Options:
  --dry-run, -d    Perform a dry run analysis without making changes
  --help, -h       Show this help message

Examples:
  node sync-all.js              # Perform full synchronization
  node sync-all.js --dry-run    # Analyze what would be synchronized
  yarn sync                     # Using yarn script
  yarn sync --dry-run           # Dry run using yarn script
`);
}

/**
 * Parses command line arguments
 */
function parseArguments() {
  const args = process.argv.slice(2);
  const options = {
    dryRun: false,
    showHelp: false
  };

  for (const arg of args) {
    switch (arg) {
      case '--dry-run':
      case '-d':
        options.dryRun = true;
        break;
      case '--help':
      case '-h':
        options.showHelp = true;
        break;
      default:
        logger.warn(`Unknown argument: ${arg}`);
        options.showHelp = true;
        break;
    }
  }

  return options;
}

/**
 * Main synchronization entry point
 */
async function main() {
  try {
    const options = parseArguments();

    if (options.showHelp) {
      showUsage();
      process.exit(0);
    }

    const credentials = {
      username: process.env.DB_REMOTE_USERNAME,
      password: process.env.DB_REMOTE_PASSWORD
    };

    if (!credentials.username || !credentials.password) {
      throw new Error(
        'Missing required environment variables: DB_REMOTE_USERNAME, DB_REMOTE_PASSWORD'
      );
    }

    if (options.dryRun) {
      const result = await syncOrchestratorService.performDryRun(credentials);
      if (!result.success) {
        process.exit(1);
      }
      process.exit(0);
    } else {
      await syncOrchestratorService.executeSync(credentials);
      process.exit(0);
    }
  } catch (error) {
    logger.error('Fatal error', error);
    process.exit(1);
  }
}

main();
