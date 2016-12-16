package jake.csvsql;

import java.util.Map;

/**
 * Listから1レコードずつ取得し、該当のレコードを削除するかどうかを判断する抽象クラス (<b>Korean</b> : List로부터 레코드를 하나씩
 * 취득하여 해당 레코드를 삭제할지 말지를 판단하는 추상 클래스)
 * 
 * @author Jake Lee
 * 
 */
public abstract class RecordDeleter {
	/**
	 * 与えられたレコードを削除するかどうかを判断する。trueを返すとレコードが削除され、falseを返すと削除されない。 (<b>Korean</b>
	 * : 주어진 레코드를 삭제할지 말지를 판단한다. true를 리턴하면 레코드가 삭제되고 false를 리턴하면 삭제되지 않는다.)
	 * 
	 * @param record
	 * @return
	 */
	public abstract boolean delete(Map record);
}
