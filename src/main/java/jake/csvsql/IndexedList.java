package jake.csvsql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class IndexedList {
	private final List<Map> list;
	private final Index index;

	IndexedList(List<Map> list, String[] keys) {
		this.list = list;
		this.index = new Index(list, keys);
	}

	List<Map> find(Conditions conditions) {
		if (conditions == null)
			return list;

		List<Map> ret = new ArrayList<Map>();
		List<Integer> idxs = index.getIndexList(conditions);
		int size = idxs != null ? idxs.size() : 0;
		for (int r = 0; r < size; r++) {
			int idx = idxs.get(r);
			ret.add(list.get(idx));
		}
		return ret;
	}

	Map findFirst(Conditions conditions) {
		if (conditions == null)
			return list.get(0);

		List<Integer> idxs = index.getIndexList(conditions);
		int size = idxs != null ? idxs.size() : 0;
		if (size == 0)
			return null;
		return list.get(idxs.get(0));
	}
}
