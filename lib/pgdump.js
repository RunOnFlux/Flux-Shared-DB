/* eslint-disable */
/**
 * PostgreSQL Dump Utility
 * Provides backup and restore functionality for PostgreSQL databases
 * Similar to mysqldump but for PostgreSQL
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const zlib = require('zlib');
const log = require('./log');

class PgDump {
  constructor(options = {}) {
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5432,
      user: options.user || 'postgres',
      password: options.password,
      database: options.database,
      
      // Dump options
      schemaOnly: options.schemaOnly || false,
      dataOnly: options.dataOnly || false,
      compressOutput: options.compressOutput || false,
      dropTables: options.dropTables || false,
      createDatabase: options.createDatabase || false,
      ifExists: options.ifExists || false,
      
      // Advanced options
      excludeTables: options.excludeTables || [],
      includeTables: options.includeTables || [],
      where: options.where || {},
      
      // Output options
      outputFile: options.outputFile,
      format: options.format || 'plain', // plain, custom, tar
    };
    
    this.pool = null;
    this.output = [];
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

  async dump() {
    try {
      await this.connect();
      
      this.addComment(`PostgreSQL database dump`);
      this.addComment(`Host: ${this.options.host}    Database: ${this.options.database}`);
      this.addComment(`Server version: PostgreSQL`);
      this.addComment(`Dump completed on ${new Date().toISOString()}`);
      this.addLine();

      // Add standard PostgreSQL settings
      this.addSetting('SET statement_timeout = 0;');
      this.addSetting('SET lock_timeout = 0;');
      this.addSetting('SET idle_in_transaction_session_timeout = 0;');
      this.addSetting('SET client_encoding = \'UTF8\';');
      this.addSetting('SET standard_conforming_strings = on;');
      this.addSetting('SET check_function_bodies = false;');
      this.addSetting('SET xmloption = content;');
      this.addSetting('SET client_min_messages = warning;');
      this.addSetting('SET row_security = off;');
      this.addLine();

      if (this.options.createDatabase) {
        await this.dumpCreateDatabase();
      }

      if (!this.options.dataOnly) {
        await this.dumpSchema();
      }

      if (!this.options.schemaOnly) {
        await this.dumpData();
      }

      await this.disconnect();

      // Write to file if specified
      if (this.options.outputFile) {
        await this.writeToFile();
      }

      return this.output.join('\n');
    } catch (error) {
      log.error(`Dump failed: ${error.message}`);
      await this.disconnect();
      throw error;
    }
  }

  async dumpCreateDatabase() {
    const client = await this.pool.connect();
    try {
      // Get database info
      const dbResult = await client.query(`
        SELECT d.datname, u.usename as owner, pg_encoding_to_char(d.encoding) as encoding
        FROM pg_database d 
        JOIN pg_user u ON d.datdba = u.usesysid 
        WHERE d.datname = $1
      `, [this.options.database]);

      if (dbResult.rows.length > 0) {
        const db = dbResult.rows[0];
        
        if (this.options.dropTables && this.options.ifExists) {
          this.addStatement(`DROP DATABASE IF EXISTS "${db.datname}";`);
        }
        
        this.addComment(`Database: ${db.datname}`);
        this.addStatement(`CREATE DATABASE "${db.datname}" WITH OWNER = "${db.owner}" ENCODING = '${db.encoding}';`);
        this.addStatement(`\\connect "${db.datname}";`);
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpSchema() {
    await this.dumpSchemas();
    await this.dumpExtensions();
    await this.dumpTables();
    await this.dumpSequences();
    await this.dumpViews();
    await this.dumpFunctions();
    await this.dumpTriggers();
    await this.dumpConstraints();
    await this.dumpIndexes();
  }

  async dumpSchemas() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
      `);

      for (const row of result.rows) {
        if (row.schema_name !== 'public') {
          this.addStatement(`CREATE SCHEMA IF NOT EXISTS "${row.schema_name}";`);
        }
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpExtensions() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT extname, extversion
        FROM pg_extension
        WHERE extname NOT IN ('plpgsql')
        ORDER BY extname
      `);

      for (const row of result.rows) {
        this.addStatement(`CREATE EXTENSION IF NOT EXISTS "${row.extname}" WITH VERSION '${row.extversion}';`);
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpTables() {
    const client = await this.pool.connect();
    try {
      let tableQuery = `
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
      `;

      if (this.options.includeTables.length > 0) {
        const tableList = this.options.includeTables.map(t => `'${t}'`).join(',');
        tableQuery += ` AND tablename IN (${tableList})`;
      }

      if (this.options.excludeTables.length > 0) {
        const tableList = this.options.excludeTables.map(t => `'${t}'`).join(',');
        tableQuery += ` AND tablename NOT IN (${tableList})`;
      }

      tableQuery += ` ORDER BY schemaname, tablename`;

      const tables = await client.query(tableQuery);

      for (const table of tables.rows) {
        await this.dumpTable(client, table.schemaname, table.tablename);
      }
    } finally {
      client.release();
    }
  }

  async dumpTable(client, schema, tableName) {
    const fullTableName = schema === 'public' ? tableName : `"${schema}"."${tableName}"`;
    
    // Drop table if requested
    if (this.options.dropTables) {
      if (this.options.ifExists) {
        this.addStatement(`DROP TABLE IF EXISTS ${fullTableName};`);
      } else {
        this.addStatement(`DROP TABLE ${fullTableName};`);
      }
    }

    // Get table definition
    const columnsResult = await client.query(`
      SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale
      FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [schema, tableName]);

    if (columnsResult.rows.length === 0) return;

    this.addComment(`Table structure for table ${fullTableName}`);
    
    let createStatement = `CREATE TABLE ${fullTableName} (\n`;
    const columnDefs = [];

    for (const col of columnsResult.rows) {
      let colDef = `  "${col.column_name}" ${this.formatDataType(col)}`;
      
      if (col.is_nullable === 'NO') {
        colDef += ' NOT NULL';
      }
      
      if (col.column_default) {
        colDef += ` DEFAULT ${col.column_default}`;
      }
      
      columnDefs.push(colDef);
    }

    createStatement += columnDefs.join(',\n') + '\n);';
    this.addStatement(createStatement);
    this.addLine();
  }

  formatDataType(column) {
    let dataType = column.data_type.toUpperCase();
    
    if (column.character_maximum_length) {
      dataType += `(${column.character_maximum_length})`;
    } else if (column.numeric_precision && column.numeric_scale !== null) {
      dataType += `(${column.numeric_precision},${column.numeric_scale})`;
    } else if (column.numeric_precision) {
      dataType += `(${column.numeric_precision})`;
    }
    
    return dataType;
  }

  async dumpSequences() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT schemaname, sequencename
        FROM pg_sequences
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, sequencename
      `);

      for (const seq of result.rows) {
        const fullSeqName = seq.schemaname === 'public' ? seq.sequencename : `"${seq.schemaname}"."${seq.sequencename}"`;
        
        const seqInfo = await client.query(`
          SELECT last_value, start_value, increment_by, max_value, min_value, cache_size, is_cycled
          FROM ${fullSeqName}
        `);

        if (seqInfo.rows.length > 0) {
          const info = seqInfo.rows[0];
          this.addStatement(`CREATE SEQUENCE ${fullSeqName} START ${info.start_value} INCREMENT ${info.increment_by} MAXVALUE ${info.max_value} MINVALUE ${info.min_value} CACHE ${info.cache_size};`);
          this.addStatement(`SELECT setval('${fullSeqName}', ${info.last_value});`);
        }
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpViews() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT schemaname, viewname, definition
        FROM pg_views
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, viewname
      `);

      for (const view of result.rows) {
        const fullViewName = view.schemaname === 'public' ? view.viewname : `"${view.schemaname}"."${view.viewname}"`;
        this.addStatement(`CREATE VIEW ${fullViewName} AS ${view.definition}`);
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpFunctions() {
    // Simplified function dump - full implementation would be more complex
    this.addComment('Functions dump not implemented in this version');
  }

  async dumpTriggers() {
    // Simplified trigger dump - full implementation would be more complex
    this.addComment('Triggers dump not implemented in this version');
  }

  async dumpConstraints() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT 
          tc.table_schema,
          tc.table_name,
          tc.constraint_name,
          tc.constraint_type,
          kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_type
      `);

      for (const constraint of result.rows) {
        const fullTableName = constraint.table_schema === 'public' ? constraint.table_name : `"${constraint.table_schema}"."${constraint.table_name}"`;
        
        if (constraint.constraint_type === 'PRIMARY KEY') {
          this.addStatement(`ALTER TABLE ${fullTableName} ADD CONSTRAINT "${constraint.constraint_name}" PRIMARY KEY ("${constraint.column_name}");`);
        } else if (constraint.constraint_type === 'UNIQUE') {
          this.addStatement(`ALTER TABLE ${fullTableName} ADD CONSTRAINT "${constraint.constraint_name}" UNIQUE ("${constraint.column_name}");`);
        }
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpIndexes() {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT 
          schemaname,
          tablename,
          indexname,
          indexdef
        FROM pg_indexes
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        AND indexdef NOT LIKE '%UNIQUE%'
        AND indexname NOT LIKE '%_pkey'
        ORDER BY schemaname, tablename, indexname
      `);

      for (const index of result.rows) {
        this.addStatement(`${index.indexdef};`);
      }
      
      if (result.rows.length > 0) {
        this.addLine();
      }
    } finally {
      client.release();
    }
  }

  async dumpData() {
    const client = await this.pool.connect();
    try {
      let tableQuery = `
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
      `;

      if (this.options.includeTables.length > 0) {
        const tableList = this.options.includeTables.map(t => `'${t}'`).join(',');
        tableQuery += ` AND tablename IN (${tableList})`;
      }

      if (this.options.excludeTables.length > 0) {
        const tableList = this.options.excludeTables.map(t => `'${t}'`).join(',');
        tableQuery += ` AND tablename NOT IN (${tableList})`;
      }

      tableQuery += ` ORDER BY schemaname, tablename`;

      const tables = await client.query(tableQuery);

      for (const table of tables.rows) {
        await this.dumpTableData(client, table.schemaname, table.tablename);
      }
    } finally {
      client.release();
    }
  }

  async dumpTableData(client, schema, tableName) {
    const fullTableName = schema === 'public' ? `"${tableName}"` : `"${schema}"."${tableName}"`;
    
    // Get column information
    const columnsResult = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [schema, tableName]);

    if (columnsResult.rows.length === 0) return;

    const columns = columnsResult.rows.map(row => `"${row.column_name}"`);
    
    // Build WHERE clause if specified
    let whereClause = '';
    if (this.options.where[tableName]) {
      whereClause = ` WHERE ${this.options.where[tableName]}`;
    }

    // Get data
    const dataResult = await client.query(`SELECT * FROM ${fullTableName}${whereClause}`);

    if (dataResult.rows.length === 0) return;

    this.addComment(`Data for table ${fullTableName}`);
    
    // Generate INSERT statements
    for (const row of dataResult.rows) {
      const values = columns.map(col => {
        const value = row[col.replace(/"/g, '')];
        if (value === null) return 'NULL';
        if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
        if (typeof value === 'boolean') return value ? 'true' : 'false';
        if (value instanceof Date) return `'${value.toISOString()}'`;
        return value;
      });

      this.addStatement(`INSERT INTO ${fullTableName} (${columns.join(', ')}) VALUES (${values.join(', ')});`);
    }
    
    this.addLine();
  }

  async writeToFile() {
    const outputPath = this.options.outputFile;
    const content = this.output.join('\n');
    
    try {
      // Ensure directory exists
      const dir = path.dirname(outputPath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      if (this.options.compressOutput) {
        const compressed = zlib.gzipSync(content);
        fs.writeFileSync(outputPath + '.gz', compressed);
        log.info(`PostgreSQL dump written to ${outputPath}.gz (compressed)`);
      } else {
        fs.writeFileSync(outputPath, content, 'utf8');
        log.info(`PostgreSQL dump written to ${outputPath}`);
      }
    } catch (error) {
      log.error(`Failed to write dump file: ${error.message}`);
      throw error;
    }
  }

  addComment(text) {
    this.output.push(`-- ${text}`);
  }

  addLine() {
    this.output.push('');
  }

  addSetting(setting) {
    this.output.push(setting);
  }

  addStatement(statement) {
    this.output.push(statement);
  }
}

module.exports = PgDump;