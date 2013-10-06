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

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;

import com.moss.jdbcdrivers.DatabaseType;

/**
 * Responsible for creating and initializing (i.e. schema creation) a blank catalog
 */
public abstract class CatalogFactory {
	private static final String QUALIFIER_NAME_KEY="catalog-name-qualifier";
	
	protected Properties properties = new Properties();
	/**
	 * Just here to limit constructor visibility
	 */
	CatalogFactory(){
		File blankslateConfigDir = new File(new File(System.getProperty("user.home")), ".blankslate");
		if(!blankslateConfigDir.exists()) blankslateConfigDir.mkdir();
		
		File configFile = new File("blankslate.properties");
		if(!configFile.exists()) configFile = new File(blankslateConfigDir, "blankslate.properties");
		
		if(configFile.exists()){
			
			try{
				properties.load(new FileInputStream(configFile));
			}catch(Exception err){
				err.printStackTrace();
				throw new IllegalStateException(err.getMessage());
			}
		}
	}
	
	public String getCatalogNameQualifier(){
		String qualifier = properties.getProperty(QUALIFIER_NAME_KEY);
		
		if(qualifier==null || qualifier.equals("")){
			UUID uuid = UUID.randomUUID();
			qualifier = uuid.toString();
		}
		
		return qualifier;
	}
	
	public static CatalogFactory getFactory(DatabaseType dbType){
		if(dbType == DatabaseType.DB_TYPE_HSQLDB) return new HsqldbCatalogFactory();
		if(dbType == DatabaseType.DB_TYPE_DERBY) return new DerbyCatalogFactory();
		if(dbType == DatabaseType.DB_TYPE_SQL_SERVER) return new MsSqlServerCatalogFactory();
		if(dbType == DatabaseType.DB_TYPE_POSTGRESQL) return new PostgresqlCatalogFactory();
		throw new IllegalArgumentException("Automatic test catalog generated not supported for " + dbType.getPrettyName());
	}
	
	public BlankslateCatalogHandle createCatalog() throws Exception {
		return createCatalog(true);
	}
	
	public abstract BlankslateCatalogHandle createCatalog(boolean deleteOnExit) throws Exception;
	
	public abstract void deleteCatalog(BlankslateCatalogHandle name) throws Exception;
}
