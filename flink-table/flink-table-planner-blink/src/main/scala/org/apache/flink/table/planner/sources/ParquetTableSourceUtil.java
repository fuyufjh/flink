package org.apache.flink.table.planner.sources;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.expressions.*;
import org.apache.flink.util.Preconditions;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Utilities for ParquetTableSource
 *
 * @author Eric Fu
 */
public class ParquetTableSourceUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ParquetTableSourceUtil.class);

	/**
	 * Converts Flink Expression to Parquet FilterPredicate.
	 */
	@Nullable
	public static FilterPredicate toParquetPredicate(Expression exp) {
		if (exp instanceof Not) {
			FilterPredicate c = toParquetPredicate(((Not) exp).child());
			if (c == null) {
				return null;
			} else {
				return FilterApi.not(c);
			}
		} else if (exp instanceof BinaryComparison) {
			BinaryComparison binComp = (BinaryComparison) exp;

			if (!isValid(binComp)) {
				// unsupported literal Type
				LOG.debug("Unsupported predict [{}] cannot be pushed to ParquetTableSource.", exp);
				return null;
			}

			boolean onRight = literalOnRight(binComp);
			Tuple2<Operators.Column, Comparable> columnPair = extractColumnAndLiteral(binComp);

			if (columnPair != null) {
				if (exp instanceof EqualTo) {
					if (columnPair.f0 instanceof Operators.IntColumn) {
						return FilterApi.eq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.LongColumn) {
						return FilterApi.eq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
						return FilterApi.eq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.FloatColumn) {
						return FilterApi.eq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.BooleanColumn) {
						return FilterApi.eq((Operators.BooleanColumn) columnPair.f0, (Boolean) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.BinaryColumn) {
						return FilterApi.eq((Operators.BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
					}
				} else if (exp instanceof NotEqualTo) {
					if (columnPair.f0 instanceof Operators.IntColumn) {
						return FilterApi.notEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.LongColumn) {
						return FilterApi.notEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
						return FilterApi.notEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.FloatColumn) {
						return FilterApi.notEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.BooleanColumn) {
						return FilterApi.notEq((Operators.BooleanColumn) columnPair.f0, (Boolean) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.BinaryColumn) {
						return FilterApi.notEq((Operators.BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
					}
				} else if (exp instanceof GreaterThan) {
					if (onRight) {
						return greaterThan(exp, columnPair);
					} else {
						lessThan(exp, columnPair);
					}
				} else if (exp instanceof GreaterThanOrEqual) {
					if (onRight) {
						return greaterThanOrEqual(exp, columnPair);
					} else {
						return lessThanOrEqual(exp, columnPair);
					}
				} else if (exp instanceof LessThan) {
					if (onRight) {
						return lessThan(exp, columnPair);
					} else {
						return greaterThan(exp, columnPair);
					}
				} else if (exp instanceof LessThanOrEqual) {
					if (onRight) {
						return lessThanOrEqual(exp, columnPair);
					} else {
						return greaterThanOrEqual(exp, columnPair);
					}
				} else {
					// Unsupported Predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into ParquetTableSource.", exp);
					return null;
				}
			}
		} else if (exp instanceof BinaryExpression) {
			if (exp instanceof And) {
				LOG.debug("All of the predicates should be in CNF. Found an AND expression.", exp);
			} else if (exp instanceof Or) {
				FilterPredicate c1 = toParquetPredicate(((Or) exp).left());
				FilterPredicate c2 = toParquetPredicate(((Or) exp).right());

				if (c1 == null || c2 == null) {
					return null;
				} else {
					return FilterApi.or(c1, c2);
				}
			} else {
				// Unsupported Predicate
				LOG.debug("Unsupported predicate [{}] cannot be pushed into ParquetTableSource.", exp);
				return null;
			}
		}

		return null;
	}

	@Nullable
	private static FilterPredicate greaterThan(Expression exp, Tuple2<Operators.Column, Comparable> columnPair) {
		Preconditions.checkArgument(exp instanceof GreaterThan, "exp has to be GreaterThan");
		if (columnPair.f0 instanceof Operators.IntColumn) {
			return FilterApi.gt((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.LongColumn) {
			return FilterApi.gt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
			return FilterApi.gt((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.FloatColumn) {
			return FilterApi.gt((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
		}

		return null;
	}

	@Nullable
	private static FilterPredicate lessThan(Expression exp, Tuple2<Operators.Column, Comparable> columnPair) {
		Preconditions.checkArgument(exp instanceof LessThan, "exp has to be LessThan");

		if (columnPair.f0 instanceof Operators.IntColumn) {
			return FilterApi.lt((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.LongColumn) {
			return FilterApi.lt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
			return FilterApi.lt((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.FloatColumn) {
			return FilterApi.lt((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
		}

		return null;
	}

	@Nullable
	private static FilterPredicate greaterThanOrEqual(Expression exp, Tuple2<Operators.Column, Comparable> columnPair) {
		Preconditions.checkArgument(exp instanceof GreaterThanOrEqual, "exp has to be GreaterThanOrEqual");
		if (columnPair.f0 instanceof Operators.IntColumn) {
			return FilterApi.gtEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.LongColumn) {
			return FilterApi.gtEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
			return FilterApi.gtEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.FloatColumn) {
			return FilterApi.gtEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
		}

		return null;
	}

	@Nullable
	private static FilterPredicate lessThanOrEqual(Expression exp, Tuple2<Operators.Column, Comparable> columnPair) {
		Preconditions.checkArgument(exp instanceof LessThanOrEqual, "exp has to be LessThanOrEqual");
		if (columnPair.f0 instanceof Operators.IntColumn) {
			return FilterApi.ltEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.LongColumn) {
			return FilterApi.ltEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
			return FilterApi.ltEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
		} else if (columnPair.f0 instanceof Operators.FloatColumn) {
			return FilterApi.ltEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
		}

		return null;
	}

	private static boolean isValid(BinaryComparison comp) {
		return (comp.left() instanceof Literal && comp.right() instanceof Attribute) ||
			(comp.left() instanceof Attribute && comp.right() instanceof Literal);
	}

	private static boolean literalOnRight(BinaryComparison comp) {
		if (comp.left() instanceof Literal && comp.right() instanceof Attribute) {
			return false;
		} else if (comp.left() instanceof Attribute && comp.right() instanceof Literal) {
			return true;
		} else {
			throw new RuntimeException("Invalid binary comparison.");
		}
	}

	private static TypeInformation<?> getLiteralType(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Literal) comp.right()).resultType();
		} else {
			return ((Literal) comp.left()).resultType();
		}
	}

	private static Object getLiteral(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Literal) comp.right()).value();
		} else {
			return ((Literal) comp.left()).value();
		}
	}

	private static String getColumnName(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Attribute) comp.left()).name();
		} else {
			return ((Attribute) comp.right()).name();
		}
	}

	@Nullable
	private static Tuple2<Operators.Column, Comparable> extractColumnAndLiteral(BinaryComparison comp) {
		TypeInformation<?> typeInfo = getLiteralType(comp);
		String columnName = getColumnName(comp);

		// fetch literal and ensure it is comparable
		Object value = getLiteral(comp);
		// validate that literal is comparable
		if (!(value instanceof Comparable)) {
			LOG.warn("Encountered a non-comparable literal of type {}." +
				"Cannot push predicate [{}] into ParquetTablesource." +
				"This is a bug and should be reported.", value.getClass().getCanonicalName(), comp);
			return null;
		}

		if (typeInfo == BasicTypeInfo.BYTE_TYPE_INFO ||
			typeInfo == BasicTypeInfo.SHORT_TYPE_INFO ||
			typeInfo == BasicTypeInfo.INT_TYPE_INFO) {
			return new Tuple2<>(FilterApi.intColumn(columnName), (Integer) value);
		} else if (typeInfo == BasicTypeInfo.LONG_TYPE_INFO) {
			return new Tuple2<>(FilterApi.longColumn(columnName), (Long) value);
		} else if (typeInfo == BasicTypeInfo.FLOAT_TYPE_INFO) {
			return new Tuple2<>(FilterApi.floatColumn(columnName), (Float) value);
		} else if (typeInfo == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
			return new Tuple2<>(FilterApi.booleanColumn(columnName), (Boolean) value);
		} else if (typeInfo == BasicTypeInfo.DOUBLE_TYPE_INFO) {
			return new Tuple2<>(FilterApi.doubleColumn(columnName), (Double) value);
		} else if (typeInfo == BasicTypeInfo.STRING_TYPE_INFO) {
			return new Tuple2<>(FilterApi.binaryColumn(columnName), Binary.fromString((String) value));
		} else {
			// unsupported type
			return null;
		}
	}
}
