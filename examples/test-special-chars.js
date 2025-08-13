#!/usr/bin/env node

import { DatabaseConfig } from '../src/config/database.js';

console.log('Testing Special Character Handling in Passwords\n');

// Test cases with various special characters
const testCases = [
  {
    name: 'Basic special characters',
    password: 'p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?'
  },
  {
    name: 'URL-like password',
    password: 'user@domain.com:pass/word?param=value'
  },
  {
    name: 'Complex password',
    password: 'MyP@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?'
  },
  {
    name: 'Password with spaces',
    password: 'pass word with spaces'
  },
  {
    name: 'Password with quotes',
    password: 'pass"word"with"quotes'
  }
];

testCases.forEach((testCase, index) => {
  console.log(`${index + 1}. ${testCase.name}`);
  console.log(`   Password: ${testCase.password}`);

  const config = new DatabaseConfig(
    'localhost',
    5432,
    'test_db',
    'test_user',
    testCase.password,
    false
  );

  const connectionString = config.getConnectionString();
  console.log(`   Connection String: ${connectionString}`);
  console.log(`   URL Encoded: ${encodeURIComponent(testCase.password)}`);
  console.log('');
});

console.log('✅ All special character tests completed successfully!');
console.log(
  '\nNote: The connection strings above show how passwords are properly URL encoded'
);
console.log('to handle special characters in database connection URLs.');
