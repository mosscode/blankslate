/**
 * Copyright (C) 2013, Moss Computing Inc.
 *
 * This file is part of blankslate.
 *
 * blankslate is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * blankslate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with blankslate; see the file COPYING.  If not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Linking this library statically or dynamically with other modules is
 * making a combined work based on this library.  Thus, the terms and
 * conditions of the GNU General Public License cover the whole
 * combination.
 *
 * As a special exception, the copyright holders of this library give you
 * permission to link this library with independent modules to produce an
 * executable, regardless of the license terms of these independent
 * modules, and to copy and distribute the resulting executable under
 * terms of your choice, provided that you also meet, for each linked
 * independent module, the terms and conditions of the license of that
 * module.  An independent module is a module which is not derived from
 * or based on this library.  If you modify this library, you may extend
 * this exception to your version of the library, but you are not
 * obligated to do so.  If you do not wish to do so, delete this
 * exception statement from your version.
 */
package com.moss.blankslate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.jdbcdrivers.JdbcConnectionConfig;

public class PostgresqlCatalogFactory extends CatalogFactory{
	private static final Log log = LogFactory.getLog(PostgresqlCatalogFactory.class);
	private static final String CONFIG_PROPERTY_KEY_HOST="postgres.host";
	private static final String CONFIG_PROPERTY_KEY_PASSWORD="postgres.saPassword";
	
	// Hard-coded assumptions that might need to be parameterized
	private static final String DEFAULT_SCHEMA_NAME = "postgres";
	private static final String LOGIN = "postgres";
	
	private static Integer sequenceNum = 1;
	
	private String hostname, password;
	
	/**
	 * Postgres doesn't always delete a catalog immediately.  See http://www.mail-archive.com/pgsql-hackers@postgresql.org/msg65284.html
	 */
	public PostgresqlCatalogFactory(){
		hostname = properties.getProperty(CONFIG_PROPERTY_KEY_HOST);
		password = properties.getProperty(CONFIG_PROPERTY_KEY_PASSWORD);
	}
	
	public BlankslateCatalogHandle createCatalog(boolean deleteOnExit) throws Exception {
		checkConfig();

		String testCatalogName = "test-" + sequenceNum + "-"+ getCatalogNameQualifier();
		
		synchronized(sequenceNum){
			sequenceNum++;
		}
		
		JdbcConnectionConfig config = new JdbcConnectionConfig(DatabaseType.DB_TYPE_POSTGRESQL, null, hostname, null, "postgres", LOGIN, password);

		Connection adminConnection = config.createConnection();
		
		if(catalogExists(testCatalogName, adminConnection)){
			log.warn("Database '" + testCatalogName + "' already exists... will be deleted");
					deleteCatalog(testCatalogName);
		}

		// THIS IS THE CONFIGURATION FOR THE AS-YET-UNCREATED CATALOG
		JdbcConnectionConfig newCatalogConfig = new JdbcConnectionConfig(DatabaseType.DB_TYPE_POSTGRESQL, null, hostname, null, testCatalogName, LOGIN, password);
		
		// CREATE THE CATALOG
		adminConnection.createStatement().execute("CREATE DATABASE \"" + testCatalogName + "\" TEMPLATE template0");
		adminConnection.close();
		assertClosed(adminConnection);
		
		// CREATE THE DEFAULT SCHEMA
		Connection newDatabaseConnection = newCatalogConfig.createConnection();
		dropSchema(DEFAULT_SCHEMA_NAME, newDatabaseConnection);
		newDatabaseConnection.createStatement().execute("create schema " + DEFAULT_SCHEMA_NAME + "");
		newDatabaseConnection.commit();
		newDatabaseConnection.close();
		assertClosed(newDatabaseConnection);
		
		BlankslateCatalogHandle handle = new BlankslateCatalogHandle(testCatalogName, newCatalogConfig, DEFAULT_SCHEMA_NAME);
		return handle;
	}

	public List getCatalogs(Connection connection) throws SQLException {
		List catalogNames = new ArrayList();
		
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("select datname from pg_catalog.pg_database");
		while (rs.next()) {
			String name = rs.getString(1);
			log.debug("Found catalog " + name);
			catalogNames.add(name);
		}
		rs.close();
		stmt.close();
		
		return catalogNames;
	}
	
	public boolean catalogExists(String catalogName, Connection connection) throws SQLException {
		//NOTE: postgres stores all catalog names as lowercase, regardless
		return getCatalogs(connection).contains(catalogName.toLowerCase());
	}

	private void assertClosed(Connection connection) throws Exception {
		if(!connection.isClosed()) throw new Exception("Connection not closed!");
	}
	private void dropSchema(String schemaName, Connection connection) throws SQLException {
		ResultSet schemas = connection.getMetaData().getSchemas();
		boolean hasSchema = false;
		while(schemas.next()){
			String nextSchema = schemas.getString("TABLE_SCHEM");
			log.debug("Found schema " + nextSchema);
			if(
					nextSchema.equals(schemaName))
				hasSchema=true;
		}
		
		if(hasSchema){
			log.debug("Dropping schema \"" + schemaName + "\"");
			connection.createStatement().execute("DROP SCHEMA " + schemaName);
			connection.commit();
		}else{
			log.debug("Could not find schema to drop: \"" + schemaName+ "\"");
		}
	}
	
	private void checkConfig() throws ConfigurationException {
		if(hostname ==null || LOGIN==null || password==null){
			String errors = "";
			if(hostname==null) errors += "\n" + CONFIG_PROPERTY_KEY_HOST;
			if(password==null) errors += "\n" + CONFIG_PROPERTY_KEY_PASSWORD;
			throw new ConfigurationException("Missing configuration information (set these properties in ~/.blankslate/blankslate.properties or ./blankslate.properties):" + errors);
		}
	}
	
	
	private void deleteCatalog(String name) throws Exception {
//		Thread.sleep(500);
		
		JdbcConnectionConfig config = new JdbcConnectionConfig(DatabaseType.DB_TYPE_POSTGRESQL, null, hostname, null, "postgres", LOGIN, password);
		Connection adminConnection = config.createConnection();
		adminConnection.createStatement().execute("drop database \"" + name + "\"");
		adminConnection.commit();
		adminConnection.close();
		assertClosed(adminConnection);
	}
	
	public void deleteCatalog(BlankslateCatalogHandle name) throws Exception {
		deleteCatalog(name.getName());
	}
}
