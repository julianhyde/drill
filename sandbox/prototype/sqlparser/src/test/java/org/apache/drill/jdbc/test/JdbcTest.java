/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.jdbc.test;

import com.google.common.base.Function;

import junit.framework.TestCase;

import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.jdbc.DrillTable;

import java.sql.*;

/** Unit tests for Drill's JDBC driver. */
public class JdbcTest extends TestCase {
  private static final String MODEL =
      "{\n"
      + "  version: '1.0',\n"
      + "   schemas: [\n"
      + "     {\n"
      + "       name: 'DONUTS',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'DONUTS',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n"
      + "         }\n"
      + "       ]\n"
      + "     },\n"
      + "     {\n"
      + "       name: 'HR',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'EMPLOYEES',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'DEPARTMENTS',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'EMP',\n"
      + "           type: 'view',\n"
      + "           sql: 'select _MAP[\\'deptId\\'] as deptid, cast(_MAP[\\'lastName\\'] as varchar) as lastName from employees'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'DEPT',\n"
      + "           type: 'view',\n"
      + "           sql: 'select _MAP[\\'deptId\\'] as deptid, _MAP[\\'name\\'] as name from departments'\n"
      + "         }\n"
      + "       ]\n"
      + "     },\n"
      + "     {\n"
      + "       name: 'FOODMART',\n"
      + "       tables: [\n"
      + "         {\n"
      + "           name: 'PRODUCT_CLASS',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n"
      + "         },\n"
      + "         {\n"
      + "           name: 'TIME_BY_DAY',\n"
      + "           type: 'custom',\n"
      + "           factory: '" + DrillTable.Factory.class.getName() + "'\n"
      + "         }\n"
      + "       ]\n"
      + "     }\n"
      + "   ]\n"
      + "}";

  private static final String EXPECTED =
      "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, id=0001, name=Cake, ppu=0.55, sales=35, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}]}, id=0002, name=Raised, ppu=0.69, sales=145, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}]}, id=0003, name=Old Fashioned, ppu=0.55, sales=300, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}, {id=1002, type=Chocolate}, {id=1003, type=Blueberry}, {id=1004, type=Devil's Food}]}, filling=[{id=6001, type=None}, {id=6002, type=Raspberry}, {id=6003, type=Lemon}, {id=6004, type=Chocolate}, {id=6005, type=Kreme}], id=0004, name=Filled, ppu=0.69, sales=14, topping=[{id=5001, type=None}, {id=5002, type=Glazed}, {id=5005, type=Sugar}, {id=5007, type=Powdered Sugar}, {id=5006, type=Chocolate with Sprinkles}, {id=5003, type=Chocolate}, {id=5004, type=Maple}], type=donut}\n"
      + "_MAP={batters={batter=[{id=1001, type=Regular}]}, id=0005, name=Apple Fritter, ppu=1.0, sales=700, topping=[{id=5002, type=Glazed}], type=donut}\n";

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/DRILL-61">DRILL-61</a>
   * is fixed. When it is fixed, remove this constant enable the tests that are
   * disabled because of it. */
  private static final boolean BUG_DRILL_61_FIXED = false;

  /** Whether
   * <a href="https://issues.apache.org/jira/browse/DRILL-61">DRILL-65</a>
   * is fixed. When it is fixed, remove this constant enable the tests that are
   * disabled because of it. */
  private static final boolean BUG_DRILL_65_FIXED = false;

  /**
   * Command-line utility to execute a logical plan.
   *
   * <p>The forwarding method ensures that the IDE calls
   * this method with the right classpath.</p>
   */
  public static void main(String[] args) throws Exception {
      ReferenceInterpreter.main(args);
  }

  /** Load driver. */
  public void testLoadDriver() throws ClassNotFoundException {
    Class.forName("org.apache.drill.jdbc.Driver");
  }

  /** Load driver and make a connection. */
  public void testConnect() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection(
        "jdbc:drill:schema=DONUTS");
    connection.close();
  }

  /** Load driver, make a connection, prepare a statement. */
  public void testPrepare() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .withConnection(
            new Function<Connection, Void>() {
              public Void apply(Connection connection) {
                try {
                  final Statement statement = connection.prepareStatement(
                      "select * from donuts");
                  statement.close();
                  return null;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }

  /** Simple query against JSON. */
  public void testSelectJson() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts")
        .returns(EXPECTED);
  }

  /** Simple query against EMP table in HR database. */
  public void testSelectEmployees() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from employees")
        .returns("_MAP={deptId=31, lastName=Rafferty}\n"
                 + "_MAP={deptId=33, lastName=Jones}\n"
                 + "_MAP={deptId=33, lastName=Steinberg}\n"
                 + "_MAP={deptId=34, lastName=Robinson}\n"
                 + "_MAP={deptId=34, lastName=Smith}\n"
                 + "_MAP={lastName=John}\n");
  }

  /** Simple query against EMP table in HR database. */
  public void testSelectEmpView() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from emp")
        .returns(
            "DEPTID=31; LASTNAME=Rafferty\n" + "DEPTID=33; LASTNAME=Jones\n"
            + "DEPTID=33; LASTNAME=Steinberg\n"
            + "DEPTID=34; LASTNAME=Robinson\n" + "DEPTID=34; LASTNAME=Smith\n"
            + "DEPTID=null; LASTNAME=John\n");
  }

  /** Simple query against EMP table in HR database. */
  public void testSelectDept() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from departments")
        .returns("_MAP={deptId=31, name=Sales}\n"
                 + "_MAP={deptId=33, name=Engineering}\n"
                 + "_MAP={deptId=34, name=Clerical}\n"
                 + "_MAP={deptId=35, name=Marketing}\n");
  }

  /** Query with project list. No field references yet. */
  public void testProjectConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select 1 + 3 as c from donuts")
        .returns("C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n");
  }

  /** Query that projects an element from the map. */
  public void testProject() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .returns("PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=1.0\n");
  }

  /** Same logic as {@link #testProject()}, but using a subquery. */
  public void testProjectOnSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['ppu'] as ppu from (\n"
             + " select _MAP as d from donuts)")
        .returns("PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=0.55\n"
            + "PPU=0.69\n"
            + "PPU=1.0\n");
  }

  /** Checks the logical plan. */
  public void testProjectPlan() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .planContains(
            "{'head':{'type':'apache_drill_logical_plan','version':'1','generator':{'type':'manual','info':'na'}},"
            + "'storage':[{'name':'donuts-json','type':'classpath'},{'name':'queue','type':'queue'}],"
            + "'query':["
            + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'},'@id':1},"
            + "{'op':'project','input':1,'projections':[{'expr':'_MAP.ppu','ref':'output.PPU'}],'@id':2},"
            + "{'op':'store','input':2,'storageengine':'queue','memo':'output sink','target':{'number':0},'@id':3}]}");
  }

  /** Query with subquery, filter, and projection of one real and one
   * nonexistent field from a map field. */
  public void testProjectFilterSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['name'] as name, d['xx'] as xx from (\n"
            + " select _MAP as d from donuts)\n"
            + "where cast(d['ppu'] as double) > 0.6")
        .returns("NAME=Raised; XX=null\n"
            + "NAME=Filled; XX=null\n"
            + "NAME=Apple Fritter; XX=null\n");
  }

  public void testProjectFilterSubqueryPlan() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select d['name'] as name, d['xx'] as xx from (\n"
            + " select _MAP['donuts'] as d from donuts)\n"
            + "where cast(d['ppu'] as double) > 0.6")
        .planContains(
            "{'head':{'type':'apache_drill_logical_plan','version':'1','generator':{'type':'manual','info':'na'}},'storage':[{'name':'donuts-json','type':'classpath'},{'name':'queue','type':'queue'}],"
            + "'query':["
            + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'},'@id':1},"
            + "{'op':'filter','input':1,'expr':'(_MAP.donuts.ppu > 0.6)','@id':2},"
            + "{'op':'project','input':2,'projections':[{'expr':'_MAP.donuts','ref':'output.D'}],'@id':3},"
            + "{'op':'project','input':3,'projections':[{'expr':'D.name','ref':'output.NAME'},{'expr':'D.xx','ref':'output.XX'}],'@id':4},"
            + "{'op':'store','input':4,'storageengine':'queue','memo':'output sink','target':{'number':0},'@id':5}]}");
  }

  /** Query that projects one field. (Disabled; uses sugared syntax.) */
  public void _testProjectNestedFieldSugared() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select donuts.ppu from donuts")
        .returns("C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n"
            + "C=4\n");
  }

  /** Query with filter. No field references yet. */
  public void testFilterConstantFalse() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts where 3 > 4")
        .returns("");
  }

  public void testFilterConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("select * from donuts where 3 < 4")
        .returns(EXPECTED);
  }

  public void testValues() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS")
        .sql("values (1)")
        .returns("EXPR$0=1\n");

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    // .planContains("store");
  }

  public void testDistinct() throws Exception {
    if (!BUG_DRILL_61_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select distinct deptId from emp")
        .returnsUnordered("DEPTID=null", "DEPTID=31", "DEPTID=34", "DEPTID=33")
        .planContains("collapse");
  }

  public void testCountNoGroupBy() throws Exception {
    if (!BUG_DRILL_61_FIXED) {
      return;
    }
    // 5 out of 6 employees have a not-null deptId
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select count(deptId) as c from emp")
        .returns("C=5\n")
        .planContains("collapse");
  }

  public void testDistinctCountNoGroupBy() throws Exception {
    if (!BUG_DRILL_61_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select count(distinct deptId) as c from emp")
        .returns("C=4\n")
        .planContains("collapse");
  }

  public void testDistinctCountGroupByEmpty() throws Exception {
    if (!BUG_DRILL_61_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select count(distinct deptId) as c from emp group by ()")
        .returns("C=4\n")
        .planContains("collapse");
  }

  public void testCount() throws Exception {
    if (!BUG_DRILL_61_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select deptId, count(*) as c from emp group by deptId")
        .returnsUnordered(
            // will change when DRILL-61 fixed
            "DEPTID=31; C=2",
            "DEPTID=33; C=2",
            "DEPTID=34; C=1")
        .planContains("collapsingaggregate"); // make sure not using optiq
  }

  public void testJoin() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from emp join dept on emp.deptId = dept.deptId")
        .returnsUnordered(
            "DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical")
        .planContains("'type':'inner'");
  }

  public void testLeftJoin() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from emp left join dept on emp.deptId = dept.deptId")
        .returnsUnordered(
            "DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null")
        .planContains("'type':'left'");
  }

  /** Right join is tricky because Drill's "join" operator only supports "left",
   * so we have to flip inputs. */
  public void _testRightJoin() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from emp right join dept on emp.deptId = dept.deptId")
        .returnsUnordered("xx")
        .planContains("'type':'left'");
  }

  public void testFullJoin() throws Exception {
    // The output is wrong, per DRILL-64. Will need to update the output when
    // that bug is fixed.
    JdbcAssert.withModel(MODEL, "HR")
        .sql("select * from emp full join dept on emp.deptId = dept.deptId")
        .returnsUnordered(
            "DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null",
            "DEPTID=null; LASTNAME=null; DEPTID0=31; NAME=Sales",
            "DEPTID=null; LASTNAME=null; DEPTID0=33; NAME=Engineering",
            "DEPTID=null; LASTNAME=null; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=null; DEPTID0=35; NAME=Marketing")
        .planContains("'type':'outer'");
  }

  /** Join on subquery; also tests that if a field of the same name exists in
   * both inputs, both fields make it through the join. */
  public void testJoinOnSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql(
            "select * from (\n"
             + "select deptId, lastname, 'x' as name from emp) as e\n"
             + " join dept on e.deptId = dept.deptId")
        .returnsUnordered(
            "DEPTID=31; LASTNAME=Rafferty; NAME=x; DEPTID0=31; NAME0=Sales",
            "DEPTID=33; LASTNAME=Jones; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=34; LASTNAME=Robinson; NAME=x; DEPTID0=34; NAME0=Clerical",
            "DEPTID=34; LASTNAME=Smith; NAME=x; DEPTID0=34; NAME0=Clerical")
        .planContains("'type':'inner'");
  }

  /** Tests that one of the FoodMart tables is present. */
  public void testFoodMart() throws Exception {
    JdbcAssert.withModel(MODEL, "FOODMART")
        .sql("select * from product_class where cast(_map['product_class_id'] as integer) < 3")
        .returnsUnordered(
            "_MAP={product_category=Seafood, product_class_id=2, product_department=Seafood, product_family=Food, product_subcategory=Shellfish}",
            "_MAP={product_category=Specialty, product_class_id=1, product_department=Produce, product_family=Food, product_subcategory=Nuts}");
  }

  public void testUnionAll() throws Exception {
    if (!BUG_DRILL_65_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql(
            "select deptId from dept\n"
            + "union all\n"
            + "select deptId from emp")
        .returnsUnordered(
            "DEPTID=31",
            "DEPTID=33",
            "DEPTID=34",
            "DEPTID=35",
            "DEPTID=null")
        .planContains("'op':'union','distinct':false");
  }

  public void testUnion() throws Exception {
    if (!BUG_DRILL_65_FIXED) {
      return;
    }
    JdbcAssert.withModel(MODEL, "HR")
        .sql(
            "select deptId from dept\n"
            + "union\n"
            + "select deptId from emp")
        .returnsUnordered(
            "DEPTID=31",
            "DEPTID=33",
            "DEPTID=34",
            "DEPTID=35",
            "DEPTID=null")
        .planContains("'op':'union','distinct':true");
  }

  public void testOrderBy() throws Exception {
    JdbcAssert.withModel(MODEL, "HR")
        .sql(
            "select * from emp order by deptId desc nulls first")
        .returns(
            "DEPTID=34; LASTNAME=Robinson\n"
            + "DEPTID=34; LASTNAME=Smith\n"
            + "DEPTID=33; LASTNAME=Jones\n"
            + "DEPTID=33; LASTNAME=Steinberg\n"
            + "DEPTID=31; LASTNAME=Rafferty\n"
            + "DEPTID=null; LASTNAME=John\n")
        .planContains("'op':'order'");
  }
}

// End JdbcTest.java
