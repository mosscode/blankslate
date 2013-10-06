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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.jdbcdrivers.JdbcConnectionConfig;

public class HsqldbCatalogFactory extends CatalogFactory {
	private static final Log log = LogFactory.getLog(HsqldbCatalogFactory.class);
	
	private static int handleNameSequenceNum = 0;
	private static List handles = new ArrayList();
	
	public BlankslateCatalogHandle createCatalog(boolean deleteOnExit) throws Exception {
		synchronized(handles){
			handleNameSequenceNum ++;
			int dbNum = handleNameSequenceNum;
			
			
			if(handles.size()>10) throw new Exception("Cannot create 11th database: HSQLDB can only serve 9 databases at a time.");
			
			log.info("Creating HSQLDB catalog temp_" + Integer.toString(dbNum) + " for testing");
			JdbcConnectionConfig connectionConfig=null;
			if(deleteOnExit){
				connectionConfig = new JdbcConnectionConfig(DatabaseType.DB_TYPE_HSQLDB, null, null, null, "mem:temp_" + Integer.toString(dbNum), "sa", "");
			}else{
				throw new Exception("PERSISTENT HSQLDB DATABASES NOT YET IMPLEMENTED \n" + 
									"(please consider doing it yourself - \n" + 
									" something should prompt for a filesystem location\n" +
									"if it can't be determined from a system property of some sort)");
			}
			

			BlankslateCatalogHandle handle = new BlankslateCatalogHandle("temp_" + Integer.toString(dbNum), connectionConfig, "PUBLIC");
			handles.add(handle);
			
			if(connectionConfig.createConnection().isClosed()){
				throw new Exception("Connection could not be made!");
			}
			return handle;
		}
	}

	public void deleteCatalog(BlankslateCatalogHandle handle) throws Exception {
		synchronized(handles){
			if(!handles.contains(handle)) throw new Exception("Invalid handle!");
			handles.remove(handle);
			
			log.info("Shutting down HSQLDB testing catalog " + handle.getName() + " (" + handle.getConfig().getJdbcUrl() + ")");
			
			Connection c = handle.getConfig().createConnection();
			c.createStatement().execute("SHUTDOWN");
			c.close();
		}
	}

}
