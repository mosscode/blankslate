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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.jdbcdrivers.DatabaseType;
import com.moss.jdbcdrivers.EnhancedJdbcConnectionConfig;
import com.moss.jdbcdrivers.JdbcConnectionConfig;

public class DerbyCatalogFactory extends CatalogFactory{
	private static final Log log = LogFactory.getLog(DerbyCatalogFactory.class);
	private static boolean hasInited=false;
	private static List handles = new ArrayList();
	private static int catalogSequenceNum=0;
	private static File derbyHome;
	
	private static void init() throws Exception {
		if(hasInited==true) return;
		hasInited=true;
		derbyHome = File.createTempFile("temp_derby_root", ".dir");
		derbyHome.delete();
		derbyHome.mkdir();
		derbyHome.deleteOnExit();
		log.info("Setting derby test home to " + derbyHome.getAbsolutePath());
		System.setProperty("derby.system.home", derbyHome.getAbsolutePath());
	}
	
	public BlankslateCatalogHandle createCatalog(boolean deleteOnExit) throws Exception {
		synchronized(handles){
			init();
			catalogSequenceNum++;
			String name = "temp_" + Integer.toString(catalogSequenceNum);
			name = new File(derbyHome, name).getAbsolutePath();
			{// MAKING THIS CONNECTION WITH THE SPECIAL 'create' PROPERTY CREATES THE DB ON DISK
				Properties props = new Properties();
				props.put("create", "true");
				JdbcConnectionConfig jdbcConfig = new JdbcConnectionConfig(DatabaseType.DB_TYPE_DERBY, null, null, null, name, "", "", props);
				java.sql.Connection connection = jdbcConfig.createConnection();
				connection.close();
			}
			JdbcConnectionConfig jdbcConfig = new EnhancedJdbcConnectionConfig(DatabaseType.DB_TYPE_DERBY, null, null, null, name, "", "");

			
			BlankslateCatalogHandle handle = new BlankslateCatalogHandle(name, jdbcConfig, "APP");
			return handle;
		}
	}

	public void deleteCatalog(BlankslateCatalogHandle name) throws Exception {
	}
	
}
