package jake.csvsql;

import java.util.Map;

/**
 * Listから1レコードずつ取得し、処理する抽象クラス (<b>Korean</b> : List로부터 레코드를 하나씩 취득하여 처리하는 추상 클래스)
 * 
 * @author Jake Lee
 * 
 */
public abstract class RecordHandler {
	/**
	 * 与えられたレコードを処理する。 (<b>Korean</b> : 주어진 레코드를 처리한다.)
	 * @param index 
	 * @param record
	 */
	public abstract void handle(int index, Map record) throws Exception;
}
