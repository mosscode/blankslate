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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.jdbcdrivers.JdbcConnectionConfig;

public class MsSqlServerCatalogFactory extends CatalogFactory{
	private static final String CONFIG_PROPERTY_KEY_HOST="mssql.host";
	private static final String CONFIG_PROPERTY_KEY_PASSWORD="mssql.saPassword";
	
	private static final Log log = LogFactory.getLog(MsSqlServerCatalogFactory.class);
	
	private static Integer sequenceNum = 1;
	
	private String hostname;
	private String login = "sa";
	private String password;
	
	MsSqlServerCatalogFactory() {
		super();
		hostname = properties.getProperty(CONFIG_PROPERTY_KEY_HOST);
		password = properties.getProperty(CONFIG_PROPERTY_KEY_PASSWORD);

	}
	
	private void checkConfig() throws ConfigurationException {
		if(hostname ==null || login==null || password==null){
			String errors = "";
			if(hostname==null) errors += "\n" + CONFIG_PROPERTY_KEY_HOST;
			if(password==null) errors += "\n" + CONFIG_PROPERTY_KEY_PASSWORD;
			throw new ConfigurationException("Missing configuration information (set these properties in ~/.blankslate/blankslate.properties or ./blankslate.properties):" + errors);
		}
	}
	
	public synchronized BlankslateCatalogHandle createCatalog(boolean deleteOnExit) throws Exception {
		checkConfig();
		
		String testCatalogName = "test-" + sequenceNum + "-"+ getCatalogNameQualifier();
		
		synchronized(sequenceNum){
			sequenceNum++;
		}
		
		JdbcConnectionConfig config = new JdbcConnectionConfig(DatabaseType.DB_TYPE_SQL_SERVER, null, hostname, null, "master", login, password);
		Connection adminConnection = config.createConnection();
		try{
			adminConnection.createStatement().execute("use \"" + testCatalogName + "\"");
			log.info("Database \"" + testCatalogName + "\" already exists... will be deleted");
			try {
				adminConnection.createStatement().execute("use master");
				adminConnection.createStatement().execute("drop database \"" + testCatalogName + "\"");
			} catch (Exception e) {
				e.printStackTrace();
				throw(e);
			}
			adminConnection.commit();
		}catch(Exception err){
		}
		adminConnection.createStatement().execute("create database \"" + testCatalogName + "\"");
		adminConnection.close();
		JdbcConnectionConfig newCatalogConfig = new JdbcConnectionConfig(DatabaseType.DB_TYPE_SQL_SERVER, null, hostname, null, testCatalogName, login, password);
		BlankslateCatalogHandle handle = new BlankslateCatalogHandle(testCatalogName, newCatalogConfig, "dbo");
		return handle;
	}
	
	public void deleteCatalog(BlankslateCatalogHandle handle) throws Exception {
		JdbcConnectionConfig config = new JdbcConnectionConfig(DatabaseType.DB_TYPE_SQL_SERVER, null, hostname, null, "master", login, password);
		Connection adminConnection = config.createConnection();
		adminConnection.createStatement().execute("use master");
		adminConnection.createStatement().execute("drop database \"" + handle.getName() + "\"");
		adminConnection.close();
	}
	
}
