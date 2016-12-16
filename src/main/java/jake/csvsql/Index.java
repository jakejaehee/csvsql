package jake.csvsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JOINの速度を向上させるために検索キーをインデックスに作成する。 <b>(Korean :</b> JOIN의 속도를 향상시키기 위하여 검색 대상 키를 인덱스로 만든다.<b>)</b>
 * 
 * @author Jake Lee
 */
class Index {
	private Map<String, List<Integer>> indexMap = new HashMap<String, List<Integer>>();
	private static final char COMMA = (char) 0x07;

	/**
	 * コンストラクタ <b>(Korean :</b> 생성자<b>)</b>
	 * 
	 * @param list
	 *            検索対象List <b>(Korean :</b> 검색 대상 List<b>)</b>
	 * @param keys
	 *            List内の検索対象キー <b>(Korean :</b> List 내의 검색 대상 키<b>)</b>
	 */
	Index(List<Map> list, String[] keys) {
		int lSize = list != null ? list.size() : 0;
		for (int r = 0; r < lSize; r++) {
			Map record = list.get(r);
			String vStr = null;
			for (int k = 0; k < keys.length; k++) {
				if (vStr == null)
					vStr = String.valueOf(record.get(keys[k]));
				else
					vStr += COMMA + String.valueOf(record.get(keys[k]));
			}
			List<Integer> idxs = indexMap.get(vStr);
			if (idxs == null) {
				idxs = new ArrayList<Integer>();
				indexMap.put(vStr, idxs);
			}
			idxs.add(r);
		}
	}

	List<Integer> getIndexList(Conditions conditions) {
		String[] values = conditions.getEqualValues();
		String vStr = null;
		for (int k = 0; k < values.length; k++) {
			if (vStr == null)
				vStr = values[k];
			else
				vStr += COMMA + values[k];
		}
		return indexMap.get(vStr);
	}
}
