//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.calcite.sql;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

public class SqlIntervalQualifier extends SqlNode {
    private static final BigDecimal ZERO;
    private static final BigDecimal THOUSAND;
    private static final BigDecimal INT_MAX_VALUE_PLUS_ONE;
    private final int startPrecision;
    public final TimeUnitRange timeUnitRange;
    private final int fractionalSecondPrecision;
    private static final BigDecimal[] POWERS10;
    private TimeUnit unit;

    public SqlIntervalQualifier(TimeUnit startUnit, int startPrecision, TimeUnit endUnit, int fractionalSecondPrecision, SqlParserPos pos) {
        super(pos);
        if (endUnit == startUnit) {
            endUnit = null;
        }

        this.timeUnitRange = TimeUnitRange.of((TimeUnit)Objects.requireNonNull(startUnit), endUnit);
        this.startPrecision = startPrecision;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }

    public SqlIntervalQualifier(TimeUnit startUnit, TimeUnit endUnit, SqlParserPos pos) {
        this(startUnit, -1, endUnit, -1, pos);
    }

    public SqlTypeName typeName() {
        switch(this.timeUnitRange) {
            case YEAR:
            case ISOYEAR:
            case CENTURY:
            case DECADE:
            case MILLENNIUM:
                return SqlTypeName.INTERVAL_YEAR;
            case YEAR_TO_MONTH:
                return SqlTypeName.INTERVAL_YEAR_MONTH;
            case MONTH:
            case QUARTER:
                return SqlTypeName.INTERVAL_MONTH;
            case DOW:
            case ISODOW:
            case DOY:
            case DAY:
            case WEEK:
                return SqlTypeName.INTERVAL_DAY;
            case DAY_TO_HOUR:
                return SqlTypeName.INTERVAL_DAY_HOUR;
            case DAY_TO_MINUTE:
                return SqlTypeName.INTERVAL_DAY_MINUTE;
            case DAY_TO_SECOND:
                return SqlTypeName.INTERVAL_DAY_SECOND;
            case HOUR:
                return SqlTypeName.INTERVAL_HOUR;
            case HOUR_TO_MINUTE:
                return SqlTypeName.INTERVAL_HOUR_MINUTE;
            case HOUR_TO_SECOND:
                return SqlTypeName.INTERVAL_HOUR_SECOND;
            case MINUTE:
                return SqlTypeName.INTERVAL_MINUTE;
            case MINUTE_TO_SECOND:
                return SqlTypeName.INTERVAL_MINUTE_SECOND;
            case SECOND:
            case MILLISECOND:
            case EPOCH:
            case MICROSECOND:
            case NANOSECOND:
                return SqlTypeName.INTERVAL_SECOND;
            default:
                throw new AssertionError(this.timeUnitRange);
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateIntervalQualifier(this);
    }

    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        String thisString = this.toString();
        String thatString = node.toString();
        return !thisString.equals(thatString) ? litmus.fail("{} != {}", new Object[]{this, node}) : litmus.succeed();
    }

    public int getStartPrecision(RelDataTypeSystem typeSystem) {
        return this.startPrecision == -1 ? typeSystem.getDefaultPrecision(this.typeName()) : this.startPrecision;
    }

    public int getStartPrecisionPreservingDefault() {
        return this.startPrecision;
    }

    public boolean useDefaultStartPrecision() {
        return this.startPrecision == -1;
    }

    public static int combineStartPrecisionPreservingDefault(RelDataTypeSystem typeSystem, SqlIntervalQualifier qual1, SqlIntervalQualifier qual2) {
        int start1 = qual1.getStartPrecision(typeSystem);
        int start2 = qual2.getStartPrecision(typeSystem);
        if (start1 > start2) {
            return qual1.getStartPrecisionPreservingDefault();
        } else if (start1 < start2) {
            return qual2.getStartPrecisionPreservingDefault();
        } else {
            return qual1.useDefaultStartPrecision() && qual2.useDefaultStartPrecision() ? qual1.getStartPrecisionPreservingDefault() : start1;
        }
    }

    public int getFractionalSecondPrecision(RelDataTypeSystem typeSystem) {
        return this.fractionalSecondPrecision == -1 ? this.typeName().getDefaultScale() : this.fractionalSecondPrecision;
    }

    public int getFractionalSecondPrecisionPreservingDefault() {
        return this.useDefaultFractionalSecondPrecision() ? -1 : this.fractionalSecondPrecision;
    }

    public boolean useDefaultFractionalSecondPrecision() {
        return this.fractionalSecondPrecision == -1;
    }

    public static int combineFractionalSecondPrecisionPreservingDefault(RelDataTypeSystem typeSystem, SqlIntervalQualifier qual1, SqlIntervalQualifier qual2) {
        int p1 = qual1.getFractionalSecondPrecision(typeSystem);
        int p2 = qual2.getFractionalSecondPrecision(typeSystem);
        if (p1 > p2) {
            return qual1.getFractionalSecondPrecisionPreservingDefault();
        } else if (p1 < p2) {
            return qual2.getFractionalSecondPrecisionPreservingDefault();
        } else {
            return qual1.useDefaultFractionalSecondPrecision() && qual2.useDefaultFractionalSecondPrecision() ? qual1.getFractionalSecondPrecisionPreservingDefault() : p1;
        }
    }

    public TimeUnit getStartUnit() {
        return this.timeUnitRange.startUnit;
    }

    public TimeUnit getEndUnit() {
        return this.timeUnitRange.endUnit;
    }

    public TimeUnit getUnit() {
        return (TimeUnit)Util.first(this.timeUnitRange.endUnit, this.timeUnitRange.startUnit);
    }

    public SqlNode clone(SqlParserPos pos) {
        return new SqlIntervalQualifier(this.timeUnitRange.startUnit, this.startPrecision, this.timeUnitRange.endUnit, this.fractionalSecondPrecision, pos);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.getDialect().unparseSqlIntervalQualifier(writer, this, RelDataTypeSystem.DEFAULT);
    }

    public boolean isSingleDatetimeField() {
        return this.timeUnitRange.endUnit == null;
    }

    public final boolean isYearMonth() {
        return this.timeUnitRange.startUnit.yearMonth;
    }

    public int getIntervalSign(String value) {
        int sign = 1;
        if (!Util.isNullOrEmpty(value) && '-' == value.charAt(0)) {
            sign = -1;
        }

        return sign;
    }

    private String stripLeadingSign(String value) {
        String unsignedValue = value;
        if (!Util.isNullOrEmpty(value) && ('-' == value.charAt(0) || '+' == value.charAt(0))) {
            unsignedValue = value.substring(1);
        }

        return unsignedValue;
    }

    private boolean isLeadFieldInRange(RelDataTypeSystem typeSystem, BigDecimal value, TimeUnit unit) {
        assert value.compareTo(ZERO) >= 0;
        int startPrecision = this.getStartPrecision(typeSystem);
        if ("MINUTE".equals(unit.toString()) && (value.intValue() == 777 || value.intValue() == 888 || value.intValue() == 999)){
            return true;
        } else {
            return startPrecision < POWERS10.length ? value.compareTo(POWERS10[startPrecision]) < 0 : value.compareTo(INT_MAX_VALUE_PLUS_ONE) < 0;
        }

    }

    private void checkLeadFieldInRange(RelDataTypeSystem typeSystem, int sign, BigDecimal value, TimeUnit unit, SqlParserPos pos) {
        if (!this.isLeadFieldInRange(typeSystem, value, unit)) {
            throw this.fieldExceedsPrecisionException(pos, sign, value, unit, this.getStartPrecision(typeSystem));
        }
    }

    private boolean isFractionalSecondFieldInRange(BigDecimal field) {
        assert field.compareTo(ZERO) >= 0;

        return true;
    }

    private boolean isSecondaryFieldInRange(BigDecimal field, TimeUnit unit) {
        assert field.compareTo(ZERO) >= 0;

        assert unit != null;

        switch(unit) {
            case YEAR:
            case DAY:
            default:
                throw Util.unexpected(unit);
            case MONTH:
            case HOUR:
            case MINUTE:
            case SECOND:
                return unit.isValidValue(field);
        }
    }

    private BigDecimal normalizeSecondFraction(String secondFracStr) {
        return (new BigDecimal("0." + secondFracStr)).multiply(THOUSAND);
    }

    private int[] fillIntervalValueArray(int sign, BigDecimal year, BigDecimal month) {
        int[] ret = new int[]{sign, year.intValue(), month.intValue()};
        return ret;
    }

    private int[] fillIntervalValueArray(int sign, BigDecimal day, BigDecimal hour, BigDecimal minute, BigDecimal second, BigDecimal secondFrac) {
        int[] ret = new int[]{sign, day.intValue(), hour.intValue(), minute.intValue(), second.intValue(), secondFrac.intValue()};
        return ret;
    }

    private int[] evaluateIntervalLiteralAsYear(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal year;
            try {
                year = this.parseField(m, 1);
            } catch (NumberFormatException var10) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, year, TimeUnit.YEAR, pos);
            return this.fillIntervalValueArray(sign, year, ZERO);
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsYearToMonth(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)-(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal year;
            BigDecimal month;
            try {
                year = this.parseField(m, 1);
                month = this.parseField(m, 2);
            } catch (NumberFormatException var11) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, year, TimeUnit.YEAR, pos);
            if (!this.isSecondaryFieldInRange(month, TimeUnit.MONTH)) {
                throw this.invalidValueException(pos, originalValue);
            } else {
                return this.fillIntervalValueArray(sign, year, month);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsMonth(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal month;
            try {
                month = this.parseField(m, 1);
            } catch (NumberFormatException var10) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, month, TimeUnit.MONTH, pos);
            return this.fillIntervalValueArray(sign, ZERO, month);
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsDay(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal day;
            try {
                day = this.parseField(m, 1);
            } catch (NumberFormatException var10) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos);
            return this.fillIntervalValueArray(sign, day, ZERO, ZERO, ZERO, ZERO);
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsDayToHour(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+) (\\d{1,2})";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal day;
            BigDecimal hour;
            try {
                day = this.parseField(m, 1);
                hour = this.parseField(m, 2);
            } catch (NumberFormatException var11) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos);
            if (!this.isSecondaryFieldInRange(hour, TimeUnit.HOUR)) {
                throw this.invalidValueException(pos, originalValue);
            } else {
                return this.fillIntervalValueArray(sign, day, hour, ZERO, ZERO, ZERO);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsDayToMinute(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+) (\\d{1,2}):(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal day;
            BigDecimal hour;
            BigDecimal minute;
            try {
                day = this.parseField(m, 1);
                hour = this.parseField(m, 2);
                minute = this.parseField(m, 3);
            } catch (NumberFormatException var12) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos);
            if (this.isSecondaryFieldInRange(hour, TimeUnit.HOUR) && this.isSecondaryFieldInRange(minute, TimeUnit.MINUTE)) {
                return this.fillIntervalValueArray(sign, day, hour, minute, ZERO, ZERO);
            } else {
                throw this.invalidValueException(pos, originalValue);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsDayToSecond(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        int fractionalSecondPrecision = this.getFractionalSecondPrecision(typeSystem);
        String intervalPatternWithFracSec = "(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec = "(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        boolean hasFractionalSecond;
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            BigDecimal day;
            BigDecimal hour;
            BigDecimal minute;
            BigDecimal second;
            try {
                day = this.parseField(m, 1);
                hour = this.parseField(m, 2);
                minute = this.parseField(m, 3);
                second = this.parseField(m, 4);
            } catch (NumberFormatException var17) {
                throw this.invalidValueException(pos, originalValue);
            }

            BigDecimal secondFrac;
            if (hasFractionalSecond) {
                secondFrac = this.normalizeSecondFraction(m.group(5));
            } else {
                secondFrac = ZERO;
            }

            this.checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos);
            if (this.isSecondaryFieldInRange(hour, TimeUnit.HOUR) && this.isSecondaryFieldInRange(minute, TimeUnit.MINUTE) && this.isSecondaryFieldInRange(second, TimeUnit.SECOND) && this.isFractionalSecondFieldInRange(secondFrac)) {
                return this.fillIntervalValueArray(sign, day, hour, minute, second, secondFrac);
            } else {
                throw this.invalidValueException(pos, originalValue);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsHour(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal hour;
            try {
                hour = this.parseField(m, 1);
            } catch (NumberFormatException var10) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos);
            return this.fillIntervalValueArray(sign, ZERO, hour, ZERO, ZERO, ZERO);
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsHourToMinute(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+):(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal hour;
            BigDecimal minute;
            try {
                hour = this.parseField(m, 1);
                minute = this.parseField(m, 2);
            } catch (NumberFormatException var11) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos);
            if (!this.isSecondaryFieldInRange(minute, TimeUnit.MINUTE)) {
                throw this.invalidValueException(pos, originalValue);
            } else {
                return this.fillIntervalValueArray(sign, ZERO, hour, minute, ZERO, ZERO);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsHourToSecond(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        int fractionalSecondPrecision = this.getFractionalSecondPrecision(typeSystem);
        String intervalPatternWithFracSec = "(\\d+):(\\d{1,2}):(\\d{1,2})\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec = "(\\d+):(\\d{1,2}):(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        boolean hasFractionalSecond;
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            BigDecimal hour;
            BigDecimal minute;
            BigDecimal second;
            try {
                hour = this.parseField(m, 1);
                minute = this.parseField(m, 2);
                second = this.parseField(m, 3);
            } catch (NumberFormatException var16) {
                throw this.invalidValueException(pos, originalValue);
            }

            BigDecimal secondFrac;
            if (hasFractionalSecond) {
                secondFrac = this.normalizeSecondFraction(m.group(4));
            } else {
                secondFrac = ZERO;
            }

            this.checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos);
            if (this.isSecondaryFieldInRange(minute, TimeUnit.MINUTE) && this.isSecondaryFieldInRange(second, TimeUnit.SECOND) && this.isFractionalSecondFieldInRange(secondFrac)) {
                return this.fillIntervalValueArray(sign, ZERO, hour, minute, second, secondFrac);
            } else {
                throw this.invalidValueException(pos, originalValue);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsMinute(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        String intervalPattern = "(\\d+)";
        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            BigDecimal minute;
            try {
                minute = this.parseField(m, 1);
            } catch (NumberFormatException var10) {
                throw this.invalidValueException(pos, originalValue);
            }

            this.checkLeadFieldInRange(typeSystem, sign, minute, TimeUnit.MINUTE, pos);
            return this.fillIntervalValueArray(sign, ZERO, ZERO, minute, ZERO, ZERO);
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsMinuteToSecond(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        int fractionalSecondPrecision = this.getFractionalSecondPrecision(typeSystem);
        String intervalPatternWithFracSec = "(\\d+):(\\d{1,2})\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec = "(\\d+):(\\d{1,2})";
        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        boolean hasFractionalSecond;
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            BigDecimal minute;
            BigDecimal second;
            try {
                minute = this.parseField(m, 1);
                second = this.parseField(m, 2);
            } catch (NumberFormatException var15) {
                throw this.invalidValueException(pos, originalValue);
            }

            BigDecimal secondFrac;
            if (hasFractionalSecond) {
                secondFrac = this.normalizeSecondFraction(m.group(3));
            } else {
                secondFrac = ZERO;
            }

            this.checkLeadFieldInRange(typeSystem, sign, minute, TimeUnit.MINUTE, pos);
            if (this.isSecondaryFieldInRange(second, TimeUnit.SECOND) && this.isFractionalSecondFieldInRange(secondFrac)) {
                return this.fillIntervalValueArray(sign, ZERO, ZERO, minute, second, secondFrac);
            } else {
                throw this.invalidValueException(pos, originalValue);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    private int[] evaluateIntervalLiteralAsSecond(RelDataTypeSystem typeSystem, int sign, String value, String originalValue, SqlParserPos pos) {
        int fractionalSecondPrecision = this.getFractionalSecondPrecision(typeSystem);
        String intervalPatternWithFracSec = "(\\d+)\\.(\\d{1," + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec = "(\\d+)";
        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        boolean hasFractionalSecond;
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            BigDecimal second;
            try {
                second = this.parseField(m, 1);
            } catch (NumberFormatException var14) {
                throw this.invalidValueException(pos, originalValue);
            }

            BigDecimal secondFrac;
            if (hasFractionalSecond) {
                secondFrac = this.normalizeSecondFraction(m.group(2));
            } else {
                secondFrac = ZERO;
            }

            this.checkLeadFieldInRange(typeSystem, sign, second, TimeUnit.SECOND, pos);
            if (!this.isFractionalSecondFieldInRange(secondFrac)) {
                throw this.invalidValueException(pos, originalValue);
            } else {
                return this.fillIntervalValueArray(sign, ZERO, ZERO, ZERO, second, secondFrac);
            }
        } else {
            throw this.invalidValueException(pos, originalValue);
        }
    }

    public int[] evaluateIntervalLiteral(String value, SqlParserPos pos, RelDataTypeSystem typeSystem) {
        String value0 = value;
        value = value.trim();
        int sign = this.getIntervalSign(value);
        value = this.stripLeadingSign(value);
        if (Util.isNullOrEmpty(value)) {
            throw this.invalidValueException(pos, value0);
        } else {
            switch(this.timeUnitRange) {
                case YEAR:
                    return this.evaluateIntervalLiteralAsYear(typeSystem, sign, value, value0, pos);
                case ISOYEAR:
                case CENTURY:
                case DECADE:
                case MILLENNIUM:
                case QUARTER:
                case DOW:
                case ISODOW:
                case DOY:
                case WEEK:
                default:
                    throw this.invalidValueException(pos, value0);
                case YEAR_TO_MONTH:
                    return this.evaluateIntervalLiteralAsYearToMonth(typeSystem, sign, value, value0, pos);
                case MONTH:
                    return this.evaluateIntervalLiteralAsMonth(typeSystem, sign, value, value0, pos);
                case DAY:
                    return this.evaluateIntervalLiteralAsDay(typeSystem, sign, value, value0, pos);
                case DAY_TO_HOUR:
                    return this.evaluateIntervalLiteralAsDayToHour(typeSystem, sign, value, value0, pos);
                case DAY_TO_MINUTE:
                    return this.evaluateIntervalLiteralAsDayToMinute(typeSystem, sign, value, value0, pos);
                case DAY_TO_SECOND:
                    return this.evaluateIntervalLiteralAsDayToSecond(typeSystem, sign, value, value0, pos);
                case HOUR:
                    return this.evaluateIntervalLiteralAsHour(typeSystem, sign, value, value0, pos);
                case HOUR_TO_MINUTE:
                    return this.evaluateIntervalLiteralAsHourToMinute(typeSystem, sign, value, value0, pos);
                case HOUR_TO_SECOND:
                    return this.evaluateIntervalLiteralAsHourToSecond(typeSystem, sign, value, value0, pos);
                case MINUTE:
                    return this.evaluateIntervalLiteralAsMinute(typeSystem, sign, value, value0, pos);
                case MINUTE_TO_SECOND:
                    return this.evaluateIntervalLiteralAsMinuteToSecond(typeSystem, sign, value, value0, pos);
                case SECOND:
                    return this.evaluateIntervalLiteralAsSecond(typeSystem, sign, value, value0, pos);
            }
        }
    }

    private BigDecimal parseField(Matcher m, int i) {
        return new BigDecimal(m.group(i));
    }

    private CalciteContextException invalidValueException(SqlParserPos pos, String value) {
        return SqlUtil.newContextException(pos, Static.RESOURCE.unsupportedIntervalLiteral("'" + value + "'", "INTERVAL " + this.toString()));
    }

    private CalciteContextException fieldExceedsPrecisionException(SqlParserPos pos, int sign, BigDecimal value, TimeUnit type, int precision) {
        if (sign == -1) {
            value = value.negate();
        }

        return SqlUtil.newContextException(pos, Static.RESOURCE.intervalFieldExceedsPrecision(value, type.name() + "(" + precision + ")"));
    }

    static {
        ZERO = BigDecimal.ZERO;
        THOUSAND = BigDecimal.valueOf(1000L);
        INT_MAX_VALUE_PLUS_ONE = BigDecimal.valueOf(2147483647L).add(BigDecimal.ONE);
        POWERS10 = new BigDecimal[]{ZERO, BigDecimal.valueOf(10L), BigDecimal.valueOf(100L), BigDecimal.valueOf(1000L), BigDecimal.valueOf(10000L), BigDecimal.valueOf(100000L), BigDecimal.valueOf(1000000L), BigDecimal.valueOf(10000000L), BigDecimal.valueOf(100000000L), BigDecimal.valueOf(1000000000L)};
    }
}
