package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private Employee mapEmployee(ResultSet resultSet) {
        try {
            BigInteger id = new BigInteger(resultSet.getString("id"));
            FullName fullName = new FullName(
                    resultSet.getString("firstname"),
                    resultSet.getString("lastname"),
                    resultSet.getString("middlename"));
            Position position = Position.valueOf(resultSet.getString("position"));
            LocalDate hireOnDate = LocalDate.parse(resultSet.getString("hiredate"));
            BigDecimal salary = resultSet.getBigDecimal("salary");
            BigInteger managerId = BigInteger.valueOf(resultSet.getInt("manager"));
            BigInteger departmentId = BigInteger.valueOf(resultSet.getInt("department"));
            return new Employee(id, fullName, position, hireOnDate, salary, managerId, departmentId);
        }
        catch (SQLException e) {
            return null;
        }
    }

    private Department mapDepartment(ResultSet resultSet) {
        try {
            BigInteger id = new BigInteger(resultSet.getString("id"));
            String name = resultSet.getString("name");
            String location = resultSet.getString("location");
            return new Department(id, name, location);
        }
        catch (SQLException e) {
            return null;
        }
    }

    private enum statementTemplateStrings {
        selectEmployeeAll {
            public String toString() {
                return "SELECT * FROM employee ";
            }
        },
        selectEmployeeById {
            public String toString() {
                return "SELECT * FROM employee WHERE id = ?";
            }
        },
        selectEmployeeByDepartment {
            public String toString() {
                return "SELECT * FROM employee WHERE department = ?";
            }
        },
        selectEmployeeByManager {
            public String toString() {
                return "SELECT * FROM employee WHERE manager = ?";
            }
        },
        insertEmployee {
            public String toString() {
                return "INSERT INTO employee VALUES (?,?,?,?,?,?,?,?,?)";
            }
        },
        updateEmployee {
            public String toString() {
                return "UPDATE employee SET firstname = ?, lastname = ?, middlename = ?, postion = ?, hiredate = ?, salary = ?, manager = ?, department = ? WHERE id = ?";
            }
        },
        deleteEmployee {
            public String toString() {
                return "DELETE FROM employee WHERE id = ?";
            }
        },

        selectDepartmentAll {
            public String toString() {
                return "SELECT * FROM department ";
            }
        },
        selectDepartmentById {
            public String toString() {
                return "SELECT * FROM department WHERE id = ?";
            }
        },
        insertDepartment {
            public String toString() {
                return "INSERT INTO department VALUES (?,?,?)";
            }
        },
        updateDepartment {
            public String toString() {
                return "UPDATE DEPARTMENT SET name = ?, location = ? WHERE id = ?";
            }
        },
        deleteDepartment {
            public String toString() {
                return "DELETE FROM department WHERE id = ?";
            }
        }
    }

    private static class SqlCloset {

        PreparedStatement statementTemplate;
        Connection connection;

        public SqlCloset()
                throws SQLException
        {
            connection = ConnectionSource.instance().createConnection();
        }
        public PreparedStatement setQueryTemplate (statementTemplateStrings option)
                throws SQLException
        {
            statementTemplate = connection.prepareStatement(option.toString());
            return statementTemplate;
        }
        public void tidyUp()
                throws SQLException
        {
            connection.commit();
            connection.close();
        }
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                List<Employee> employeeDepartmentList = new ArrayList<>();
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.selectEmployeeByDepartment);
                    queryTemplate.setInt(1, department.getId().intValue());
                    ResultSet queryResultTable = queryTemplate.executeQuery();
                    while(queryResultTable.next())
                    {
                        employeeDepartmentList.add(mapEmployee(queryResultTable));
                    }
                }
                catch (SQLException e)
                {
                    return null;
                }
                return employeeDepartmentList;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> employeeManagertList = new ArrayList<>();
                SqlCloset db;
                try
                {
                    db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.selectEmployeeByManager);
                    queryTemplate.setInt(1, employee.getId().intValue());
                    ResultSet queryResultTable = queryTemplate.executeQuery();
                    while(queryResultTable.next())
                    {
                        employeeManagertList.add(mapEmployee(queryResultTable));
                    }
                    db.tidyUp();
                }
                catch (SQLException e)
                {
                    return null;
                }

                return employeeManagertList;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.selectEmployeeById);
                    queryTemplate.setInt(1, Id.intValue());
                    ResultSet queryResultTable = queryTemplate.executeQuery();
                    System.out.println(queryTemplate.toString());
                    while(queryResultTable.next())
                    {
                        return Optional.ofNullable(mapEmployee(queryResultTable));
                    }
                }
                catch (SQLException e)
                {
                    System.out.println("SQLException Employee by Id");
                    System.out.println(e.getErrorCode());
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    return Optional.empty();
                }
                return Optional.empty();
            }

            @Override
            public List<Employee> getAll() {
                List<Employee> employeeManagertList = new ArrayList<>();
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.selectEmployeeAll);
                    ResultSet queryResultTable = queryTemplate.executeQuery();
                    while(queryResultTable.next())
                    {
                        employeeManagertList.add(mapEmployee(queryResultTable));
                    }
                    db.tidyUp();
                }
                catch (SQLException e)
                {
                    return null;
                }
                return employeeManagertList;
            }

            @Override
            public Employee save(Employee employee) {
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate;
                    if (!getById(employee.getId()).equals(Optional.empty()))
                    {
                        PreparedStatement queryTemplateUpdate = db.setQueryTemplate(statementTemplateStrings.updateEmployee);
                        queryTemplateUpdate.setInt(9, employee.getId().intValue());
                        queryTemplateUpdate.setString(1, employee.getFullName().getFirstName());
                        queryTemplateUpdate.setString(2, employee.getFullName().getLastName());
                        queryTemplateUpdate.setString(3, employee.getFullName().getMiddleName());
                        queryTemplateUpdate.setString(4, employee.getPosition().toString());
                        queryTemplateUpdate.setInt(5, employee.getManagerId().intValue());
                        queryTemplateUpdate.setDate(6, Date.valueOf(employee.getHired()));
                        queryTemplateUpdate.setDouble(7, employee.getSalary().doubleValue());
                        queryTemplateUpdate.setInt(8, employee.getDepartmentId().intValue());
                        queryTemplate = queryTemplateUpdate;
                        queryTemplate.executeUpdate();
                    }
                    else
                    {
                        PreparedStatement queryTemplateUpdate = db.setQueryTemplate(statementTemplateStrings.insertEmployee);
                        queryTemplateUpdate.setInt(1, employee.getId().intValue());
                        queryTemplateUpdate.setString(2, employee.getFullName().getFirstName());
                        queryTemplateUpdate.setString(3, employee.getFullName().getLastName());
                        queryTemplateUpdate.setString(4, employee.getFullName().getMiddleName());
                        queryTemplateUpdate.setString(5, employee.getPosition().toString());
                        queryTemplateUpdate.setInt(6, employee.getManagerId().intValue());
                        queryTemplateUpdate.setDate(7, Date.valueOf(employee.getHired()));
                        queryTemplateUpdate.setDouble(8, employee.getSalary().doubleValue());
                        queryTemplateUpdate.setInt(9, employee.getDepartmentId().intValue());
                        queryTemplate = queryTemplateUpdate;
                        queryTemplate.executeUpdate();
                    }
                    db.tidyUp();
                    return employee;
                }
                catch (SQLException e)
                {
                    return null;
                }
            }

            @Override
            public void delete(Employee employee) {
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.deleteEmployee);
                    queryTemplate.setInt(1, employee.getId().intValue());
                    queryTemplate.executeUpdate();
                    db.tidyUp();
                }
                catch (SQLException e)
                {
                    throw new IllegalStateException();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                Department result = null;
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement queryTemplate = db.setQueryTemplate(statementTemplateStrings.selectDepartmentById);
                    queryTemplate.setInt(1, Id.intValue());
                    System.out.println(queryTemplate.toString());
                    ResultSet resultTable = queryTemplate.executeQuery();
                    System.out.println(resultTable.toString());
                    while (resultTable.next()) {
                        result = mapDepartment(resultTable);
                    }
                }
                catch (SQLException e)
                {
                    System.out.println("SQLException Dept by Id");
                    System.out.println(e.getErrorCode());
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    return Optional.empty();
                }
                return Optional.ofNullable(result);
            }

            @Override
            public List<Department> getAll() {
                List<Department> departmentList = new ArrayList<>();
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement query = db.setQueryTemplate(statementTemplateStrings.selectDepartmentAll);
                    ResultSet departmentsTable = query.executeQuery();
                    while (departmentsTable.next())
                    {
                        departmentList.add(mapDepartment(departmentsTable));
                    }
                    db.tidyUp();
                }
                catch (SQLException e)
                {
                    return null;
                }
                return departmentList;
            }

            @Override
            public Department save(Department department) {
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement statementTemplate;
                    if (getById(department.getId()).equals(Optional.empty()))
                    {
                        statementTemplate = db.setQueryTemplate(statementTemplateStrings.insertDepartment);
                        statementTemplate.setInt(1, department.getId().intValue());
                        statementTemplate.setString(2, department.getName());
                        statementTemplate.setString(3, department.getLocation());
                    }
                    else
                    {
                        statementTemplate = db.setQueryTemplate(statementTemplateStrings.updateDepartment);
                        statementTemplate.setInt(3, department.getId().intValue());
                        statementTemplate.setString(1, department.getName());
                        statementTemplate.setString(2, department.getLocation());
                    }
                    statementTemplate.executeUpdate();
                    db.tidyUp();
                    return department;
                }
                catch(SQLException e)
                {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                try
                {
                    SqlCloset db = new SqlCloset();
                    PreparedStatement statement = db.setQueryTemplate(statementTemplateStrings.deleteDepartment);
                    statement.setInt(1, department.getId().intValue());
                    statement.executeUpdate();
                    db.tidyUp();
                }
                catch (SQLException e)
                {
                    throw new IllegalStateException();
                }
            }
        };
    }
}
