package jake.csvsql;

import java.util.ArrayList;
import java.util.List;

/**
 * カラム対カラムに値をコピーする時、原本カラムと対象カラムのキーとして使用される。例: {@link ListSql#copyColumn(List, Conditions, CopyKeys)}。(<b>Korean</b> : 칼럼 대 칼럼으로 값을 복사할 때 원본 칼럼과 대상 칼럼의 키로서 사용된다. 예:
 * {@link ListSql#copyColumn(List, Conditions, CopyKeys)}.})
 * 
 * @author Jake Lee
 */
public class CopyKeys {
	private List<String[]> keys = new ArrayList<String[]>();

	public CopyKeys() {
	}

	/**
	 * コンストラクタ (<b>Korean</b> : 생성자)
	 * 
	 * @param dstKey
	 *            対象カラムのキー (<b>Korean</b> : 대상 칼럼의 키)
	 * @param srcKey
	 *            原本カラムのキー (<b>Korean</b> : 원본 칼럼의 키)
	 */
	public CopyKeys(String dstKey, String srcKey) {
		and(dstKey, srcKey);
	}

	/**
	 * コピーするカラムを追加する。(<b>Korean</b> : 복사할 칼럼을 추가한다.)
	 * 
	 * @param dstKey
	 *            対象カラムのキー (<b>Korean</b> : 대상 칼럼의 키)
	 * @param srcKey
	 *            原本カラムのキー (<b>Korean</b> : 원본 칼럼의 키)
	 * @return
	 */
	public CopyKeys and(String dstKey, String srcKey) {
		keys.add(new String[] { dstKey, srcKey });
		return this;
	}

	/**
	 * キーの数(<b>Korean</b> : 키의 갯수)
	 * 
	 * @return
	 */
	public int size() {
		return keys.size();
	}

	/**
	 * 与えられたインデックスに該当する左List<Map>のキーを返却する。(<b>Korean</b> : 주어진 인덱스에 해당하는 왼쪽
	 * List<Map>의 키를 반환한다.)
	 * 
	 * @param idx
	 *            設定されたキーの順に0から1ずつカウントアップする。(<b>Korean</b> : 설정된 키의 순으로 0부터 1씩
	 *            증가한다.)
	 * @return
	 */
	public String getDestinationKey(int idx) {
		return idx >= 0 && idx < keys.size() ? keys.get(idx)[0] : null;
	}

	/**
	 * 与えられたインデックスに該当する右List<Map>のキーを返却する。(<b>Korean</b> : 주어진 인덱스에 해당하는 오른쪽
	 * List<Map>의 키를 반환한다.)
	 * 
	 * @param idx
	 *            設定されたキーの順に0から1ずつカウントアップする。(<b>Korean</b> : 설정된 키의 순으로 0부터 1씩
	 *            증가한다.)
	 * @return
	 */
	public String getSourceKey(int idx) {
		return idx >= 0 && idx < keys.size() ? keys.get(idx)[1] : null;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int k = 0; k < keys.size(); k++) {
			if (k > 0)
				sb.append(" and ");
			sb.append(getDestinationKey(k)).append("<-")
					.append(getSourceKey(k));
		}
		return sb.toString();
	}
}
