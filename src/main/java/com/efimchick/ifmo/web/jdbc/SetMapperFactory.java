package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> doskaPochyota = new HashSet<>();
            try {
                while (resultSet.next())
                    doskaPochyota.add(getEmployeeDataRecursive(resultSet));

                return doskaPochyota;
            }
            catch (SQLException e) {
                return null;
            }
        };
    }

    private Employee getEmployeeDataRecursive(ResultSet resultSet) {
        try {
            BigInteger id = new BigInteger(resultSet.getString("id"));
            FullName fullName = new FullName(
                    resultSet.getString("firstname"),
                    resultSet.getString("lastname"),
                    resultSet.getString("middlename"));
            Position position = Position.valueOf(resultSet.getString("position"));
            LocalDate hireOnDate = LocalDate.parse(resultSet.getString("hiredate"));
            BigDecimal salary = resultSet.getBigDecimal("salary");
            Employee manager = null;
            String managerIdString = resultSet.getString("manager");

            if (managerIdString != null) {
                BigInteger managerId = new BigInteger(managerIdString);
                int anchor = resultSet.getRow();
                resultSet.absolute(0);
                while(resultSet.next() && manager == null) {
                    if(managerId.equals(new BigInteger(resultSet.getString("id")))) {
                        manager = getEmployeeDataRecursive(resultSet);
                    }
                }
                resultSet.absolute(anchor);
            }

            return new Employee(id, fullName, position, hireOnDate, salary, manager);
        }
        catch (SQLException e) {
            return null;
        }
    }
}
