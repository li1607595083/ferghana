package org_change.org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * @desc 新增加的类, 下面的方法会调用此类
 *  org.apache.flink.connector.jdbc.dialect.OracleSQLDialect#getRowConverter(org.apache.flink.table.types.logical.RowType)
 */
public class OracleSQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Oracle";
    }

    public OracleSQLRowConverter(RowType rowType) {
        super(rowType);
    }
}