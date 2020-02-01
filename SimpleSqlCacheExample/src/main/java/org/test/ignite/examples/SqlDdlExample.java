/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.test.ignite.examples;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlDdlExample {
	static Logger logger = LoggerFactory.getLogger(SqlDdlExample.class);
	/** Dummy cache name. */
	private static final String DUMMY_CACHE_NAME = "dummy_cache";

	public static void main(String[] args) throws Exception {
		try (Ignite ignite = Ignition.start("example-ignite.xml")) {
			logger.info("Cache query DDL example started.");

			CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");

			try (IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg)) {
				// Create reference City table based on REPLICATED template.
				cache.query(new SqlFieldsQuery(
						"CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll();

				// Create table based on PARTITIONED template with one backup.
				cache.query(new SqlFieldsQuery("CREATE TABLE Salaries (" + "    ID CHAR(200), "
						+ "    EmployeeName CHAR(200), " + "    JobTitle CHAR(200), " + "    BasePay CHAR(200), "
						+ "    OvertimePay CHAR(200), " + "    PRIMARY KEY (ID) "
						+ ") WITH \"template=partitioned, backups=1, CACHE_NAME=Salaries\"")).getAll();

				logger.info("Created database objects.");
				// Create an index.
				// cache.query(new SqlFieldsQuery("CREATE INDEX on Salaries
				// (city_id)")).getAll();
				List<String> queryList = new LinkedList<String>();
				try {
					Scanner sc = new Scanner(new File("Salaries.csv"));
					// reading header line
					if (sc.hasNext())
						sc.nextLine();
					while (sc.hasNext()) {
						String str = sc.nextLine();
						str =str.replaceAll("'", "''");
						String[] strArray = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
						String strInsertQUery = "Insert into Salaries (ID,EmployeeName,JobTitle,BasePay,OvertimePay) Values('"
								+ strArray[0] + "','" + strArray[1] + "','" + strArray[2] + "','" + strArray[3] +  "','" + strArray[4] + "' )";
						logger.info(strInsertQUery);
						cache.query(new SqlFieldsQuery(strInsertQUery)).getAll();
						
						// System.out.println(str);
					}
					sc.close();
				} catch (IOException e) { // TODO Auto-generated catch block
					e.printStackTrace();
				}

				logger.info("Created database objects.");

				/*
				 * SqlFieldsQuery qry = new
				 * SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)"); cache.query(new
				 * SqlFieldsQuery("")); qry = new
				 * SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)");
				 * logger.info("Populated data."); List<List<?>> res = cache.query( new
				 * SqlFieldsQuery("SELECT p.name, c.name FROM Person p INNER JOIN City c on c.id = p.city_id"
				 * )) .getAll();
				 * 
				 * logger.info("Query results:"); for (Object next : res) logger.info(">>>    "
				 * + next); cache.query(new SqlFieldsQuery("drop table Person")).getAll();
				 * cache.query(new SqlFieldsQuery("drop table City")).getAll();
				 * logger.info("Dropped database objects.");
				 */
			} finally {
				// Distributed cache can be removed from cluster only by #destroyCache() call.
				ignite.destroyCache(DUMMY_CACHE_NAME);
			}
			logger.info("Cache query DDL example finished.");
		}
	}
}
