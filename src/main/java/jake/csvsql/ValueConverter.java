package jake.csvsql;

/**
 * カラムの値を変換する時使用するコンバーター。参照:
 * {@link ListSql#copyColumn(java.util.List, String, String, ValueConverter)}
 * XXXXX <b>(Korean :</b> 칼럼의 값을 변환할 때 사용하는 변환기. 참조:
 * {@link ListSql#copyColumn(java.util.List, String, String, ValueConverter)}
 * <b>)</b>
 * 
 * <p>
 * <b>Example :</b> 
 * <xmp> ListSql.copyColumn(src, "M09_M09_ERP_ACTSALES_TEMP",
 * 	M09_M09_ERP_ACTSALES.ITEM, new ValueConverter() { 
 * 		public Object convert(Object srcValue) { 
 * 			return srcValue + "#*"; 
 *		} 
 *	}); </xmp>
 * 
 * @author Jake Lee
 */
public abstract class ValueConverter {

	/**
	 * 値を変換して返す。 <b>(Korean :</b> 값을 변환하여 반환한다.<b>)</b>
	 * 
	 * @param srcValue
	 *            原本の値 <b>(Korean :</b> 원본 값.<b>)</b>
	 * @return
	 */
	public abstract Object convert(Object srcValue);
}
