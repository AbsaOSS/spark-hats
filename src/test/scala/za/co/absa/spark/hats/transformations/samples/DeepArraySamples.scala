/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.hats.transformations.samples

// Examples for constructing dataframes containing arrays of various levels of nesting
// Also it includes error column to test transformations that can fail per field

// The case classes were declared at the package level so it can be used to create Spark DataSets
// It is declared package private so the names won't pollute public/exported namespace

// Structs of Structs example
private[transformations] case class Address(city: String, street: String)

private[transformations] case class Employee(name: String, address: Address)

private[transformations] case class TestObj(id: Int, employee: Employee)

private[transformations] case class TestObj2(id: Int, employee: Seq[Employee])

// Arrays of primitives example
private[transformations] case class FunWords(id: Int, words: Seq[String])

// Arrays of arrays of primitives example
private[transformations] case class GeoData(id: Int, matrix: Seq[Seq[String]])

// Arrays of structs example
private[transformations] case class Person(firstName: String, lastName: String)

private[transformations] case class Team(id: Int, person: Seq[Person])

private[transformations] case class Dept(name: String, team: Team)

// Arrays of Arrays of struct
private[transformations] case class Tournament(id: Int, person: Seq[Seq[Person]])

// Arrays of structs in arrays of structs
private[transformations] case class Condition(conif: String, conthen: String, amount: Double)

private[transformations] case class Leg(legid: Int, conditions: Seq[Condition])

private[transformations] case class Trade(id: Int, legs: Seq[Leg])

// Structs of Structs example with errror column
private[transformations] case class AddressWithErrColumn(city: String, street: String, buildingNum: Int, zip: String,
                                                         errors: Seq[ErrorMessage])

private[transformations] case class AddressNoErrColumn(city: String, street: String, buildingNum: Int, zip: String)

private[transformations] case class EmployeeNoErrorColumn(name: String, address: AddressNoErrColumn)

private[transformations] case class TestObj1WithErrorColumn(id: Int, employee: EmployeeNoErrorColumn, errors:
Seq[ErrorMessage])

private[transformations] case class TestObj2WithErrorColumn(id: Int, employee: Seq[EmployeeNoErrorColumn],
                                                            errors: Seq[ErrorMessage])

private[transformations] case class TestObj2NoErrColumn(id: Int, employee: Seq[EmployeeNoErrorColumn])

// Arrays of primitives example with error column
private[transformations] case class FunNumbersWithErrorColumn(id: Int, nums: Seq[String], errors: Seq[ErrorMessage])

// Arrays of arrays of primitives example with error column
private[transformations] case class GeoDataWithErrorColumn(id: Int, matrix: Seq[Seq[String]], errors: Seq[ErrorMessage])

// Arrays of structs in arrays of structs with error column
private[transformations] case class TradeWithErrorColumn(id: Int, legs: Seq[Leg], errors: Seq[ErrorMessage])

object DeepArraySamples {
  // scalastyle:off magic.number
  // scalastyle:off line.size.limit

  // WITHOUT error column

  // Plain
  val plainSampleN: Seq[Address] = Seq(
    Address("Olomuc", "Vodickova"),
    Address("Ostrava", "Vlavska"),
    Address("Plzen", "Kralova")
  )

  // Struct of struct
  val structOfStructSampleN: Seq[TestObj] = Seq(
    TestObj(1, Employee("Martin", Address("Olomuc", "Vodickova"))),
    TestObj(1, Employee("Petr", Address("Ostrava", "Vlavska"))),
    TestObj(1, Employee("Vojta", Address("Plzen", "Kralova")))
  )

  // Array of struct of struct
  val arrayOfstructOfStructSampleN: Seq[TestObj2] = Seq(
    TestObj2(1, Seq(Employee("Martin", Address("Olomuc", "Vodickova")), Employee("Stephan", Address("Olomuc", "Vodickova")))),
    TestObj2(2, Seq(Employee("Petr", Address("Ostrava", "Vlavska")), Employee("Michal", Address("Ostrava", "Vlavska")))),
    TestObj2(3, Seq(Employee("Vojta", Address("Plzen", "Kralova"))))
  )

  // Arrays of primitives
  val arraysOfPrimitivesSampleN: Seq[FunWords] = Seq(
    FunWords(1, Seq("Gizmo", "Blurp", "Buzinga")),
    FunWords(1, Seq("Quirk", "Zap", "Mmrnmhrm"))
  )

  // Arrays of arrays of primitives
  val arraysOfArraysOfPrimitivesSampleN: Seq[GeoData] = Seq(
    GeoData(1, Seq(Seq("Tree", "Table"), Seq("Map", "Duck"))),
    GeoData(2, Seq(Seq("Apple", "Machine"), Seq("List", "Duck"))),
    GeoData(3, Seq(Seq("Computer", "Snake"), Seq("Sun", "Star")))
  )

  // Arrays of structs
  val arraysOfStructsSampleN: Seq[Team] = Seq(
    Team(1, Seq(Person("John", "Smith"), Person("Jack", "Brown"))),
    Team(1, Seq(Person("Merry", "Cook"), Person("Jane", "Clark")))
  )

  // Arrays of arrays of struct
  val arraysOfArraysOfStructSampleN: Seq[Tournament] = Seq(
    Tournament(1, Seq(Seq(Person("Mona Lisa", "Harddrive")), Seq(Person("Lenny", "Linux"), Person("Dot", "Not")))),
    Tournament(1, Seq(Seq(Person("Eddie", "Larrison")), Seq(Person("Scarlett", "Johanson"), Person("William", "Windows"))))
  )

  // Arrays of struct with arrays of struct
  val arraysOfStrtuctsDeepSampleN: Seq[Trade] = Seq(
    Trade(1, Seq(
      Leg(100, Seq(
        Condition("if bid>10", "buy", 100), Condition("if sell<5", "sell", 150), Condition("if sell<1", "sell", 1000))),
      Leg(101, Seq(
        Condition("if bid<50", "sell", 200), Condition("if sell>30", "buy", 175), Condition("if sell>25", "buy", 225)))
    )),
    Trade(2, Seq(
      Leg(102, Seq(
        Condition("if bid>11", "buy", 100), Condition("if sell<6", "sell", 150), Condition("if sell<2", "sell", 1000))),
      Leg(103, Seq(
        Condition("if bid<51", "sell", 200), Condition("if sell>31", "buy", 175), Condition("if sell>26", "buy", 225)))
    )),
    Trade(3, Seq(
      Leg(104, Seq(
        Condition("if bid>12", "buy", 100), Condition("if sell<7", "sell", 150), Condition("if sell<3", "sell", 1000))),
      Leg(105, Seq(
        Condition("if bid<52", "sell", 200), Condition("if sell>32", "buy", 175), Condition("if sell>27", "buy", 225)))
    ))
  )

  // WITH error column

  // Plain
  val plainSampleE: Seq[AddressWithErrColumn] = Seq(
    AddressWithErrColumn("Olomuc", "Vodickova", 12, "12000", Seq()),
    AddressWithErrColumn("Ostrava", "Vlavska", 110, "1455a", Seq()),
    AddressWithErrColumn("Plzen", "Kralova", 71, "b881",
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value"))))
  )

  // Struct of struct
  val structOfStructSampleE: Seq[TestObj1WithErrorColumn] = Seq(
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Martin", AddressNoErrColumn("Olomuc", "Vodickova", 12, "12000")), Nil),
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Petr", AddressNoErrColumn("Ostrava", "Vlavska", 110, "1455a")),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    TestObj1WithErrorColumn(1, EmployeeNoErrorColumn("Vojta", AddressNoErrColumn("Plzen", "Kralova", 71, "b881")), Nil)
  )

  // Array of struct of struct
  val arrayOfStructOfStructErrSampleE: Seq[TestObj2WithErrorColumn] = Seq(
    TestObj2WithErrorColumn(1, Seq(
      EmployeeNoErrorColumn("Martin", AddressNoErrColumn("Olomuc", "Vodickova", 732, "73200")),
      EmployeeNoErrorColumn("Stephan", AddressNoErrColumn("Olomuc", "Vodickova", 77, "77-333"))), Nil),
    TestObj2WithErrorColumn(2, Seq(
      EmployeeNoErrorColumn("Petr", AddressNoErrColumn("Ostrava", "Vlavska", 25, "a9991")),
      EmployeeNoErrorColumn("Michal", AddressNoErrColumn("Ostrava", "Vlavska", 334, "552-aa1"))),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    TestObj2WithErrorColumn(3, Seq(
      EmployeeNoErrorColumn("Vojta", AddressNoErrColumn("Plzen", "Kralova", 33, "993"))), Nil)
  )

  val arrayOfStructOfStruvtNoErrSampleE: Seq[TestObj2NoErrColumn] = Seq(
    TestObj2NoErrColumn(1, Seq(
      EmployeeNoErrorColumn("Martin", AddressNoErrColumn("Olomuc", "Vodickova", 732, "73200")),
      EmployeeNoErrorColumn("Stephan", AddressNoErrColumn("Olomuc", "Vodickova", 77, "77-333")))),
    TestObj2NoErrColumn(2, Seq(
      EmployeeNoErrorColumn("Petr", AddressNoErrColumn("Ostrava", "Vlavska", 25, "a9991")),
      EmployeeNoErrorColumn("Michal", AddressNoErrColumn("Ostrava", "Vlavska", 334, "552-aa1")))),
    TestObj2NoErrColumn(3, Seq(
      EmployeeNoErrorColumn("Vojta", AddressNoErrColumn("Plzen", "Kralova", 33, "993"))))
  )

  // Arrays of primitives
  val arraysOfPrimitivesSampleE: Seq[FunNumbersWithErrorColumn] = Seq(
    FunNumbersWithErrorColumn(1, Seq("7755", "a212", "222-111"),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    FunNumbersWithErrorColumn(1, Seq("223a", "223a", "775"), Nil),
    FunNumbersWithErrorColumn(1, Seq("5", "-100", "9999999"), Nil)
  )

  // Arrays of arrays of primitives
  val arraysOfArraysOfPrimitivesSampleE: Seq[GeoDataWithErrorColumn] = Seq(
    GeoDataWithErrorColumn(1, Seq(Seq("10", "11b"), Seq("11b", "12")),
      Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
    GeoDataWithErrorColumn(2, Seq(Seq("20f", "300"), Seq("1000", "10-10")), Nil),
    GeoDataWithErrorColumn(3, Seq(Seq("775", "223"), Seq("100", "0")), Nil)
  )

  // Arrays of struct with arrays of struct
  val arraysOfStrtuctsDeepSampleE: Seq[TradeWithErrorColumn] = Seq(
    TradeWithErrorColumn(1, Seq(
      Leg(100, Seq(
        Condition("if bid>10", "100", 100), Condition("if sell<5", "300a", 150), Condition("if sell<1", "1000", 1000))),
      Leg(101, Seq(
        Condition("if bid<50", "200", 200), Condition("if sell>30", "175b", 175), Condition("if sell>25", "225-225", 225)))
    ), Nil),
    TradeWithErrorColumn(2, Seq(
      Leg(102, Seq(
        Condition("if bid>11", "100", 100), Condition("if sell<6", "150", 150), Condition("if sell<2", "1000", 1000))),
      Leg(103, Seq(
        Condition("if bid<51", "200", 200), Condition("if sell>31", "175", 175), Condition("if sell>26", "225", 225)))
    ), Nil),
    TradeWithErrorColumn(3, Seq(
      Leg(104, Seq(
        Condition("if bid>12", "1OO", 100), Condition("if sell<7", "150x", 150), Condition("if sell<3", "-1000-", 1000))),
      Leg(105, Seq(
        Condition("if bid<52", "2OO", 200), Condition("if sell>32", "f175", 175), Condition("if sell>27", "225_", 225)))
    ), Seq(ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value"))))
  )
}
