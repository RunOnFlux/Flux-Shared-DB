/* eslint-disable */
/**
 * PostgreSQL Import Utility
 * Provides import functionality for PostgreSQL SQL dump files
 * Similar to mysqlimport but for PostgreSQL
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { EventEmitter } = require('events');
const log = require('./log');

class PgImporter extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5432,
      user: options.user || 'postgres',
      password: options.password,
      database: options.database,
      
      // Import options
      encoding: options.encoding || 'utf8',
      maxSqlSize: options.maxSqlSize || 1024 * 1024, // 1MB per query
      skipErrors: options.skipErrors || false,
      verbose: options.verbose || false,
      
      // Callbacks
      callback: options.callback || null,
      serverSocket: options.serverSocket || null,
    };
    
    this.pool = null;
    this.imported = [];
    this.totalQueries = 0;
    this.executedQueries = 0;
    this.errors = [];
    this.currentFile = null;
    this.totalBytes = 0;
    this.processedBytes = 0;
  }

  async connect() {
    try {
      this.pool = new Pool({
        host: this.options.host,
        port: this.options.port,
        user: this.options.user,
        password: this.options.password,
        database: this.options.database,
        connectionTimeoutMillis: 15000,
        max: 5, // Limit concurrent connections during import
      });

      // Test connection
      const client = await this.pool.connect();
      client.release();
      log.info(`Connected to PostgreSQL database: ${this.options.database}`);
    } catch (error) {
      log.error(`Failed to connect to PostgreSQL: ${error.message}`);
      throw error;
    }
  }

  async disconnect() {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
  }

  setEncoding(encoding) {
    this.options.encoding = encoding;
  }

  onProgress(callback) {
    this.on('progress', callback);
  }

  getImported() {
    return this.imported;
  }

  async import(filePath) {
    try {
      if (!fs.existsSync(filePath)) {
        throw new Error(`File not found: ${filePath}`);
      }

      this.currentFile = filePath;
      const stats = fs.statSync(filePath);
      this.totalBytes = stats.size;
      this.processedBytes = 0;
      
      log.info(`Starting PostgreSQL import from: ${filePath}`);
      log.info(`File size: ${(this.totalBytes / 1024 / 1024).toFixed(2)} MB`);

      await this.connect();

      // Read and process file
      await this.processFile(filePath);

      await this.disconnect();

      this.imported.push({
        file: filePath,
        queries: this.executedQueries,
        errors: this.errors.length,
        success: this.errors.length === 0
      });

      log.info(`PostgreSQL import completed: ${this.executedQueries} queries executed, ${this.errors.length} errors`);
      
      return {
        totalQueries: this.totalQueries,
        executedQueries: this.executedQueries,
        errors: this.errors,
        success: this.errors.length === 0
      };
    } catch (error) {
      log.error(`Import failed: ${error.message}`);
      await this.disconnect();
      throw error;
    }
  }

  async processFile(filePath) {
    const content = fs.readFileSync(filePath, this.options.encoding);
    const queries = this.parseQueries(content);
    
    this.totalQueries = queries.length;
    log.info(`Found ${this.totalQueries} queries to execute`);

    const client = await this.pool.connect();
    
    try {
      // Start transaction for better performance and atomicity
      await client.query('BEGIN');
      
      for (let i = 0; i < queries.length; i++) {
        const query = queries[i];
        
        try {
          await this.executeQuery(client, query);
          this.executedQueries++;
          
          // Update progress
          this.processedBytes += query.length;
          this.emit('progress', {
            total_queries: this.totalQueries,
            executed_queries: this.executedQueries,
            total_bytes: this.totalBytes,
            bytes_processed: this.processedBytes,
            current_query: i + 1,
            percentage: ((i + 1) / this.totalQueries * 100).toFixed(2)
          });

          if (this.options.verbose && (i + 1) % 100 === 0) {
            log.info(`Executed ${i + 1}/${this.totalQueries} queries (${((i + 1) / this.totalQueries * 100).toFixed(1)}%)`);
          }

        } catch (error) {
          this.errors.push({
            query: query.substring(0, 200) + (query.length > 200 ? '...' : ''),
            error: error.message,
            line: i + 1
          });
          
          log.error(`Query ${i + 1} failed: ${error.message}`);
          log.error(`Query: ${query.substring(0, 200)}${query.length > 200 ? '...' : ''}`);
          
          if (!this.options.skipErrors) {
            throw error;
          }
        }

        // Add small delay every 50 queries to prevent overwhelming the server
        if ((i + 1) % 50 === 0) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      }

      await client.query('COMMIT');
      log.info('Transaction committed successfully');
      
    } catch (error) {
      await client.query('ROLLBACK');
      log.error('Transaction rolled back due to error');
      throw error;
    } finally {
      client.release();
    }
  }

  async executeQuery(client, query) {
    // Skip empty queries and comments
    const trimmedQuery = query.trim();
    if (!trimmedQuery || trimmedQuery.startsWith('--') || trimmedQuery.startsWith('/*')) {
      return;
    }

    // Handle special PostgreSQL commands
    if (trimmedQuery.startsWith('\\')) {
      // Skip psql meta-commands like \connect, \d, etc.
      log.debug(`Skipping psql meta-command: ${trimmedQuery}`);
      return;
    }

    // Execute query
    if (this.options.callback && typeof this.options.callback === 'function') {
      // Use callback if provided (for integration with existing system)
      await this.options.callback(query);
    } else {
      // Direct execution
      await client.query(query);
    }
  }

  parseQueries(content) {
    const queries = [];
    let currentQuery = '';
    let inString = false;
    let inComment = false;
    let inMultiLineComment = false;
    let stringChar = '';
    
    const lines = content.split('\n');
    
    for (let lineNum = 0; lineNum < lines.length; lineNum++) {
      const line = lines[lineNum];
      
      for (let i = 0; i < line.length; i++) {
        const char = line[i];
        const nextChar = line[i + 1] || '';
        const prevChar = line[i - 1] || '';
        
        // Handle multi-line comments
        if (!inString && !inComment && char === '/' && nextChar === '*') {
          inMultiLineComment = true;
          i++; // Skip next character
          continue;
        }
        
        if (inMultiLineComment && char === '*' && nextChar === '/') {
          inMultiLineComment = false;
          i++; // Skip next character
          continue;
        }
        
        if (inMultiLineComment) {
          continue;
        }
        
        // Handle single-line comments
        if (!inString && char === '-' && nextChar === '-') {
          inComment = true;
          continue;
        }
        
        if (inComment) {
          continue; // Skip rest of line
        }
        
        // Handle strings
        if (!inComment && (char === '"' || char === "'")) {
          if (!inString) {
            inString = true;
            stringChar = char;
          } else if (char === stringChar && prevChar !== '\\') {
            inString = false;
            stringChar = '';
          }
        }
        
        // Handle statement terminator
        if (!inString && !inComment && char === ';') {
          currentQuery += char;
          const trimmedQuery = currentQuery.trim();
          
          if (trimmedQuery && !trimmedQuery.startsWith('--')) {
            queries.push(trimmedQuery);
          }
          
          currentQuery = '';
          continue;
        }
        
        currentQuery += char;
      }
      
      // Reset comment flag at end of line
      inComment = false;
      
      // Add newline to preserve formatting
      if (currentQuery.trim()) {
        currentQuery += '\n';
      }
    }
    
    // Add final query if exists
    const finalQuery = currentQuery.trim();
    if (finalQuery && !finalQuery.startsWith('--')) {
      queries.push(finalQuery);
    }
    
    return queries;
  }

  // Method to import from string content instead of file
  async importFromString(content) {
    try {
      this.totalBytes = content.length;
      this.processedBytes = 0;
      
      log.info(`Starting PostgreSQL import from string content`);
      log.info(`Content size: ${(this.totalBytes / 1024).toFixed(2)} KB`);

      await this.connect();

      const queries = this.parseQueries(content);
      this.totalQueries = queries.length;
      
      log.info(`Found ${this.totalQueries} queries to execute`);

      const client = await this.pool.connect();
      
      try {
        await client.query('BEGIN');
        
        for (let i = 0; i < queries.length; i++) {
          const query = queries[i];
          
          try {
            await this.executeQuery(client, query);
            this.executedQueries++;
            
            this.processedBytes += query.length;
            this.emit('progress', {
              total_queries: this.totalQueries,
              executed_queries: this.executedQueries,
              total_bytes: this.totalBytes,
              bytes_processed: this.processedBytes,
              current_query: i + 1,
              percentage: ((i + 1) / this.totalQueries * 100).toFixed(2)
            });

          } catch (error) {
            this.errors.push({
              query: query.substring(0, 200) + (query.length > 200 ? '...' : ''),
              error: error.message,
              line: i + 1
            });
            
            if (!this.options.skipErrors) {
              throw error;
            }
          }
        }

        await client.query('COMMIT');
        
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      await this.disconnect();

      return {
        totalQueries: this.totalQueries,
        executedQueries: this.executedQueries,
        errors: this.errors,
        success: this.errors.length === 0
      };
    } catch (error) {
      log.error(`Import from string failed: ${error.message}`);
      await this.disconnect();
      throw error;
    }
  }
}

module.exports = PgImporter;