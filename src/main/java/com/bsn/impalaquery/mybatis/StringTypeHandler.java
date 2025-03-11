package com.bsn.impalaquery.mybatis;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.math.BigInteger;
import java.sql.*;

public class StringTypeHandler implements TypeHandler<Object>{

	@Override
	public void setParameter(PreparedStatement ps, int i, Object parameter, JdbcType jdbcType) throws SQLException {
		if(parameter instanceof String) {
			try {
				Integer param = Integer.parseInt(parameter.toString());
				ps.setInt(i, param);
			} catch (NumberFormatException e) {
				BigInteger param = new BigInteger(parameter.toString());
				ps.setObject(i, param, Types.BIGINT);
			}
		} else if(parameter instanceof Integer) {
			ps.setInt(i, (Integer) parameter);
		} else if(parameter instanceof BigInteger) {
			ps.setObject(i, (BigInteger) parameter, Types.BIGINT);
		} else {
			ps.setObject(i, parameter);
		}
	}

	@Override
	public String getResult(ResultSet rs, String columnName) throws SQLException {
		return rs.getString(columnName);
	}

	@Override
	public String getResult(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getString(columnIndex);
	}

	@Override
	public String getResult(CallableStatement cs, int columnIndex) throws SQLException {
		return cs.getString(columnIndex);
	}

}
