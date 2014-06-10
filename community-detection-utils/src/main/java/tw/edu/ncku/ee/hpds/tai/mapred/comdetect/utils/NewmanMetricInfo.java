package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.utils;

public class NewmanMetricInfo {
	
	private String minNodeId;
	private String maxNodeId;
	private String totInfluenceSum;
	private String currentMergeI;
	private String currentMergeJ;
	
	public NewmanMetricInfo(){
		minNodeId = new String("0");
		maxNodeId = new String("0");
		totInfluenceSum = new String("0");
		currentMergeI = new String("0");
		currentMergeJ = new String("0");
	}
	
	public NewmanMetricInfo(String MIN_NODE_ID,
							String MAX_NODE_ID,
							String TOTAL_INFLUENCE_SUM,
							String CURRENT_MERGE_I,
							String CURRENT_MERGE_J) {
		
		if(MIN_NODE_ID == null){
			minNodeId = new String("0");
		} else {
			minNodeId = new String(MIN_NODE_ID);
		}
		
		if(MAX_NODE_ID == null){
			maxNodeId = new String("0");
		} else {
			maxNodeId = new String(MAX_NODE_ID);
		}
		
		if(TOTAL_INFLUENCE_SUM == null){
			totInfluenceSum = new String("0");
		} else {
			totInfluenceSum = new String(TOTAL_INFLUENCE_SUM);
		}
		
		if(CURRENT_MERGE_I == null){
			currentMergeI = new String("0");
		} else {
			currentMergeI = new String(CURRENT_MERGE_I);
		}
		
		if(CURRENT_MERGE_J == null){
			currentMergeJ = new String("0");
		} else {
			currentMergeJ = new String(CURRENT_MERGE_J);
		}
	}
	
	public void updateMinNodeId(String MIN_NODE_ID){
		minNodeId = MIN_NODE_ID;
	}
	
	public void updateMaxNodeId(String MAX_NODE_ID){
		maxNodeId = MAX_NODE_ID;
	}
	
	public void updateTotInfluenceSum(String TOTAL_INFLUENCE_SUM){
		totInfluenceSum = TOTAL_INFLUENCE_SUM;
	}
	
	public void updateCurrentMergeI(String CURRENT_MERGE_I){
		currentMergeI = CURRENT_MERGE_I;
	}
	
	public void updateCurrentMergeJ(String CURRENT_MERGE_J){
		currentMergeJ = CURRENT_MERGE_J;
	}
	
	public void update(String MIN_NODE_ID,
						String MAX_NODE_ID,
						String TOTAL_INFLUENCE_SUM,
						String CURRENT_MERGE_I,
						String CURRENT_MERGE_J){
		
		updateMinNodeId(MIN_NODE_ID);
		updateMaxNodeId(MAX_NODE_ID);
		updateTotInfluenceSum(TOTAL_INFLUENCE_SUM);
		updateCurrentMergeI(CURRENT_MERGE_I);
		updateCurrentMergeJ(CURRENT_MERGE_J);
	}
	
	@Override
	public String toString(){
		return minNodeId + "," + maxNodeId + "," + totInfluenceSum + "," + currentMergeI + "," + currentMergeJ;
	}
}
