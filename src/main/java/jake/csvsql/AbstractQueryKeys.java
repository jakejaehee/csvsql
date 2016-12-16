package jake.csvsql;

import java.util.ArrayList;
import java.util.List;

/**
 * ListSqlで使用されるキーの抽象クラス (<b>Korean</b> : ListSql에서 사용되는 키의 추상 클래스)
 * 
 * @author Jake Lee
 * 
 */
public abstract class AbstractQueryKeys {

	private List<KeyAndSort> keys = new ArrayList<KeyAndSort>();
	private int nextPos = 0;

	public AbstractQueryKeys() {
	}

	/**
	 * コンストラクタ、ソートは昇順になる。(<b>Korean</b> : 생성자. 정렬은 오름차순이 된다.)
	 * 
	 * @param key キー名 (<b>Korean</b> : 키 이름)
	 */
	public AbstractQueryKeys(String key) {
		and(key, true);
	}

	/**
	 * コンストラクタ (<b>Korean</b> : 생성자)
	 * 
	 * @param key キー名 (<b>Korean</b> : 키 이름)
	 * @param ascendSort ソート方式。trueの場合は昇順、falseの場合は降順 (<b>Korean</b> : 정렬 방식. true이면 올림차순, false이면 내림차순)
	 */
	public AbstractQueryKeys(String key, boolean ascendSort) {
		and(key, ascendSort);
	}

	/**
	 * キー及びソート方式追加 (<b>Korean</b> : 키 및 정렬방식 추가)
	 * 
	 * @param keyAndSort
	 */
	protected void add(KeyAndSort keyAndSort) {
		keys.add(keyAndSort);
	}

	/**
	 * キー追加。ソート方式は昇順 (<b>Korean</b> : 키 추가. 정렬방식은 오름차순.)
	 * 
	 * @param key (<b>Korean</b> : 키 이름)
	 * @return
	 */
	public abstract AbstractQueryKeys and(String key);

	/**
	 * キー追加 (<b>Korean</b> : 키 추가)
	 * 
	 * @param key キー名 (<b>Korean</b> : 키 이름)
	 * @param ascendSort ソート方式。trueの場合は昇順、falseの場合は降順 (<b>Korean</b> : 정렬 방식. true이면 올림차순, false이면 내림차순)
	 * @return
	 */
	public abstract AbstractQueryKeys and(String key, boolean ascendSort);

	/**
	 * キーの数 (<b>Korean</b> : 키의 갯수)
	 * 
	 * @return
	 */
	public int size() {
		return keys.size();
	}

	/**
	 * next() メソッドの呼び出しによって返却させるキーの位置(0から1ずつカウントアップ)を0に初期化する。 (<b>Korean</b> :
	 * next() 메쏘드를 호출하면 반환될 키의 위치(0부터 1씩 증가)를 0으로 초기화한다.)
	 */
	public void resetPosition() {
		nextPos = 0;
	}

	/**
	 * 現位置にキーがあるかどうかを確認する。(<b>Korean</b> : 현재위치에 키가 있는지 여부를 확인한다.)
	 * 
	 * @return
	 */
	public boolean hasNext() {
		return nextPos < keys.size();
	}

	/**
	 * 現在のキーを返却し、キーの位置を1ずつカウントアップする。(<b>Korean</b> : 현재의 키를 반환하고 키의 위치를 1 증가
	 * 시킨다.)
	 * 
	 * @return
	 */
	public KeyAndSort next() {
		return keys.get(nextPos++);
	}

	/**
	 * 与えられたpositionのキーを返却する。(<b>Korean</b> : 주어진 position의 키를 반환한다.)
	 * 
	 * @param position
	 * @return
	 */
	public KeyAndSort get(int position) {
		if (position < 0 || position >= keys.size()) {
			throw new RuntimeException("Position " + position
					+ " is out of boundary");
		}
		return keys.get(position);
	}

	/**
	 * すべてのキーを文字列(String[])に作成し、返却する。(<b>Korean</b> : 모든 키를 문자열 배열(String[])로
	 * 만들어서 반환한다.)
	 * 
	 * @return
	 */
	public String[] getKeys() {
		String[] arr = new String[keys.size()];
		for (int k = 0; k < keys.size(); k++) {
			KeyAndSort ks = keys.get(k);
			arr[k] = ks.getKey();
		}
		return arr;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int k = 0; k < keys.size(); k++) {
			if (k > 0)
				sb.append(", ");
			sb.append(keys.get(k));
		}
		return sb.toString();
	}
}
